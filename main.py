import asyncio
import time
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api.provider import ProviderRequest
from astrbot.api import logger

from .storage import ChatChargeStorage


class ChatChargePlugin(Star):
    """聊天收费插件：按次 / 包天 / 包月订阅，支持私聊与群聊独立计费"""

    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.storage = ChatChargeStorage(self)

        # 并发安全：每个 scope 一把异步锁
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, scope: str) -> asyncio.Lock:
        if scope not in self._locks:
            self._locks[scope] = asyncio.Lock()
        return self._locks[scope]

    def _scope_key(self, is_group: bool, id_str: str) -> str:
        return f"group:{id_str}" if is_group else f"private:{id_str}"

    # ---------- 配置读取 ----------
    def _mode(self) -> str:
        mode = self.config.get("mode", "per_msg")
        if not isinstance(mode, str) or mode not in ("per_msg", "subscription"):
            logger.warning(f"[ChatCharge] 异常 mode={mode!r}，回退 per_msg")
            return "per_msg"
        return mode

    def _price_per_msg(self) -> int:
        try:
            return max(0, int(self.config.get("price_per_msg", 1)))
        except (TypeError, ValueError):
            return 1

    def _subscribe_prices(self) -> dict[str, int]:
        raw = self.config.get("subscribe_prices", [])
        if not isinstance(raw, list):
            logger.warning("[ChatCharge] subscribe_prices 非 list，回退默认")
            return {"7天": 10, "30天": 30, "365天": 299}
        result: dict[str, int] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            label = item.get("label", "")
            price = item.get("price", 0)
            if label:
                try:
                    result[str(label)] = max(0, int(price))
                except (TypeError, ValueError):
                    pass
        return result if result else {"7天": 10, "30天": 30, "365天": 299}

    def _free_trial_count(self) -> int:
        try:
            return max(0, int(self.config.get("free_trial_count", 10)))
        except (TypeError, ValueError):
            return 10

    def _admin_ids(self) -> list[str]:
        raw = self.config.get("admin_ids", [])
        if isinstance(raw, list):
            return [str(uid) for uid in raw if uid is not None]
        if isinstance(raw, str):
            return [raw]
        return []

    def _whitelist_ids(self) -> list[str]:
        raw = self.config.get("whitelist_ids", [])
        if isinstance(raw, list):
            return [str(uid) for uid in raw if uid is not None]
        if isinstance(raw, str):
            return [raw]
        return []

    def _is_admin(self, user_id: str) -> bool:
        return user_id in self._admin_ids()

    def _reply_tpl(self, key: str) -> str:
        config_key = f"tpl_{key}"
        value = self.config.get(config_key)
        defaults = {
            "balance_short": " 余额不足！当前余额: {balance}，本次需: {price}。\n请联系管理员充值。",
            "balance_info": " 您的余额: {balance}",
            "group_balance_info": " 群公共余额: {balance}",
            "sub_expired": " 您的订阅已过期，请续费。\n{price_list}\n联系管理员。",
            "group_sub_expired": " 群订阅已过期，请续费。\n{price_list}\n联系管理员。",
            "sub_info": " 订阅到期: {expire_str}（剩余 {remain_days:.1f} 天）",
            "group_sub_info": " 群订阅到期: {expire_str}（剩余 {remain_days:.1f} 天）",
            "charge_success": " 已为 {target} 充值 {amount}，当前余额: {new_balance}",
            "deduct_success": " 已从 {target} 扣除 {amount}，当前余额: {new_balance}",
            "sub_add_success": " 已为 {target} 增加 {days} 天订阅，到期: {expire_str}",
        }
        if not isinstance(value, str):
            return defaults.get(key, key)
        return value

    # ---------- 核心计费逻辑 ----------
    async def _check_and_deduct(
        self,
        user_id: str,
        group_id: str | None,
        is_group_chat: bool
    ) -> tuple[bool, str]:
        """
        检查并扣费。
        返回: (是否允许, 拦截消息)
        扣费优先级: 个人余额 > 群公共余额
        """
        mode = self._mode()

        if mode == "subscription":
            # 订阅模式：先查个人，再查群
            personal_expire = await self.storage.get_expire("private", user_id)
            if time.time() < personal_expire:
                return True, ""

            if group_id and is_group_chat:
                group_expire = await self.storage.get_expire("group", group_id)
                if time.time() < group_expire:
                    return True, ""

            # 都过期了，返回拦截消息
            prices = self._subscribe_prices()
            price_list = "\n".join(f"  · {k}: {v}" for k, v in prices.items())
            key = "group_sub_expired" if (group_id and is_group_chat) else "sub_expired"
            msg = self._reply_tpl(key).format(price_list=price_list)
            return False, msg

        # per_msg 模式
        price = self._price_per_msg()
        if price <= 0:
            return True, ""

        # 个人免费额度
        personal_count = await self.storage.get_msg_count("private", user_id)
        free_trial = self._free_trial_count()
        if personal_count < free_trial:
            await self.storage.inc_msg_count("private", user_id)
            return True, ""

        # 个人余额
        personal_balance = await self.storage.get_balance("private", user_id)
        if personal_balance >= price:
            await self.storage.set_balance("private", user_id, personal_balance - price)
            await self.storage.inc_msg_count("private", user_id)
            logger.info(f"[ChatCharge] 用户 {user_id} 个人扣费 {price}，余额: {personal_balance - price}")
            return True, ""

        # 群聊场景：查群公共余额
        if group_id and is_group_chat:
            group_balance = await self.storage.get_balance("group", group_id)
            if group_balance >= price:
                await self.storage.set_balance("group", group_id, group_balance - price)
                logger.info(f"[ChatCharge] 群 {group_id} 公共扣费 {price}，余额: {group_balance - price}")
                return True, ""

        # 都没钱，拦截
        total_balance = personal_balance
        if group_id:
            total_balance += await self.storage.get_balance("group", group_id)
        msg = self._reply_tpl("balance_short").format(balance=total_balance, price=price)
        return False, msg

    # ---------- LLM 拦截钩子 ----------
    @filter.on_llm_request()
    async def on_llm_check(self, event: AstrMessageEvent, req: ProviderRequest):
        user_id = event.get_sender_id()
        if not user_id:
            logger.warning("[ChatCharge] get_sender_id() 返回空，跳过")
            return

        if self._should_skip(user_id):
            return

        group_id = event.get_group_id() or None
        is_group_chat = bool(group_id)

        scope = self._scope_key(is_group_chat, group_id or user_id)
        lock = self._lock(scope)

        async with lock:
            allowed, msg = await self._check_and_deduct(user_id, group_id, is_group_chat)
            if not allowed:
                try:
                    await event.send(event.plain_result(msg))
                except Exception:
                    pass
                try:
                    event.stop_event()
                except Exception:
                    pass

    def _should_skip(self, user_id: str) -> bool:
        if not user_id:
            return False
        return user_id in self._whitelist_ids() or user_id in self._admin_ids()

    # ---------- 用户指令 ----------
    @filter.command("balance")
    async def cmd_balance(self, event: AstrMessageEvent):
        """查询余额 / 订阅状态"""
        user_id = event.get_sender_id()
        if not user_id:
            yield event.plain_result("⚠ 无法获取用户ID")
            return

        mode = self._mode()
        group_id = event.get_group_id()
        is_group_chat = bool(group_id)
        lines = []

        if mode == "subscription":
            personal_expire = await self.storage.get_expire("private", user_id)
            if time.time() < personal_expire:
                remain = (personal_expire - time.time()) / 86400.0
                expire_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(personal_expire))
                lines.append(self._reply_tpl("sub_info").format(
                    expire_str=expire_str, remain_days=remain
                ))
            else:
                lines.append(" 个人订阅已过期")

            if is_group_chat and group_id:
                group_expire = await self.storage.get_expire("group", group_id)
                if time.time() < group_expire:
                    remain = (group_expire - time.time()) / 86400.0
                    expire_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(group_expire))
                    lines.append(self._reply_tpl("group_sub_info").format(
                        expire_str=expire_str, remain_days=remain
                    ))
                else:
                    lines.append(" 群订阅已过期")
        else:
            personal_balance = await self.storage.get_balance("private", user_id)
            lines.append(self._reply_tpl("balance_info").format(balance=personal_balance))

            if is_group_chat and group_id:
                group_balance = await self.storage.get_balance("group", group_id)
                lines.append(self._reply_tpl("group_balance_info").format(balance=group_balance))

        yield event.plain_result("\n".join(lines))

    @filter.command("收费价格")
    async def cmd_price(self, event: AstrMessageEvent):
        """查询当前价格"""
        mode = self._mode()
        if mode == "subscription":
            prices = self._subscribe_prices()
            if not prices:
                yield event.plain_result("⚠ 未配置订阅价格")
                return
            lines = [" 订阅价格："] + [f"  · {k}: {v}" for k, v in prices.items()]
            yield event.plain_result("\n".join(lines))
        else:
            price = self._price_per_msg()
            free = self._free_trial_count()
            yield event.plain_result(f" 每条消息 {price}（前 {free} 条免费试用）")

    # ---------- 管理员指令：个人 ----------
    @filter.command("充值")
    async def cmd_charge(self, event: AstrMessageEvent, user: str, amount: int):
        """给个人充值 /充值 user_id amount"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not user or amount <= 0:
            yield event.plain_result("⚠ 请指定用户ID和正数金额")
            return

        balance = await self.storage.get_balance("private", user)
        new_balance = balance + amount
        await self.storage.set_balance("private", user, new_balance)
        await self.storage.mark_balance_user("private", user)
        msg = self._reply_tpl("charge_success").format(
            target=f"用户 {user}", amount=amount, new_balance=new_balance
        )
        yield event.plain_result(msg)

    @filter.command("扣费")
    async def cmd_deduct(self, event: AstrMessageEvent, user: str, amount: int):
        """扣个人余额 /扣费 user_id amount"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not user or amount <= 0:
            yield event.plain_result("⚠ 请指定用户ID和正数金额")
            return

        balance = await self.storage.get_balance("private", user)
        new_balance = max(0, balance - amount)
        await self.storage.set_balance("private", user, new_balance)
        msg = self._reply_tpl("deduct_success").format(
            target=f"用户 {user}", amount=amount, new_balance=new_balance
        )
        yield event.plain_result(msg)

    @filter.command("添加订阅")
    async def cmd_add_sub(self, event: AstrMessageEvent, user: str, days: int):
        """给个人添加订阅 /添加订阅 user_id 30"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not user or days <= 0:
            yield event.plain_result("⚠ 请指定用户ID和正数天数")
            return

        old_expire = await self.storage.get_expire("private", user)
        new_expire = max(old_expire, time.time()) + days * 86400.0
        await self.storage.set_expire("private", user, new_expire)
        await self.storage.mark_sub_user("private", user)
        expire_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(new_expire))
        msg = self._reply_tpl("sub_add_success").format(
            target=f"用户 {user}", days=days, expire_str=expire_str
        )
        yield event.plain_result(msg)

    # ---------- 管理员指令：群 ----------
    @filter.command("群充值")
    async def cmd_group_charge(self, event: AstrMessageEvent, group: str, amount: int):
        """给群充值公共余额 /群充值 group_id amount"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not group or amount <= 0:
            yield event.plain_result("⚠ 请指定群ID和正数金额")
            return

        balance = await self.storage.get_balance("group", group)
        new_balance = balance + amount
        await self.storage.set_balance("group", group, new_balance)
        await self.storage.mark_balance_user("group", group)
        msg = self._reply_tpl("charge_success").format(
            target=f"群 {group}", amount=amount, new_balance=new_balance
        )
        yield event.plain_result(msg)

    @filter.command("群扣费")
    async def cmd_group_deduct(self, event: AstrMessageEvent, group: str, amount: int):
        """扣群公共余额 /群扣费 group_id amount"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not group or amount <= 0:
            yield event.plain_result("⚠ 请指定群ID和正数金额")
            return

        balance = await self.storage.get_balance("group", group)
        new_balance = max(0, balance - amount)
        await self.storage.set_balance("group", group, new_balance)
        msg = self._reply_tpl("deduct_success").format(
            target=f"群 {group}", amount=amount, new_balance=new_balance
        )
        yield event.plain_result(msg)

    @filter.command("群添加订阅")
    async def cmd_group_add_sub(self, event: AstrMessageEvent, group: str, days: int):
        """给群添加订阅 /群添加订阅 group_id 30"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return
        if not group or days <= 0:
            yield event.plain_result("⚠ 请指定群ID和正数天数")
            return

        old_expire = await self.storage.get_expire("group", group)
        new_expire = max(old_expire, time.time()) + days * 86400.0
        await self.storage.set_expire("group", group, new_expire)
        await self.storage.mark_sub_user("group", group)
        expire_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(new_expire))
        msg = self._reply_tpl("sub_add_success").format(
            target=f"群 {group}", days=days, expire_str=expire_str
        )
        yield event.plain_result(msg)

    # ---------- 统计 ----------
    @filter.command("查看统计")
    async def cmd_stats(self, event: AstrMessageEvent):
        """查看插件统计信息"""
        if not self._is_admin(event.get_sender_id()):
            yield event.plain_result(" 权限不足，仅管理员可操作")
            return

        stats = await self.storage.get_stats()
        yield event.plain_result(
            f"📊 统计:\n"
            f"  · 私聊有余额用户: {stats['private_balance']} 人\n"
            f"  · 私聊有订阅用户: {stats['private_sub']} 人\n"
            f"  · 群有公共余额: {stats['group_balance']} 个\n"
            f"  · 群有订阅: {stats['group_sub']} 个"
        )

    async def terminate(self):
        pass
