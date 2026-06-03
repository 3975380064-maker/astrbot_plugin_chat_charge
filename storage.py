import asyncio
import time
from astrbot.api import logger


class ChatChargeStorage:
    """聊天收费插件存储层：区分私聊与群聊数据（兼容旧版 key）"""

    # 迁移版本号，每次需要重新迁移时递增
    MIGRATE_VERSION = 1

    def __init__(self, plugin):
        self.plugin = plugin
        self._migrate_lock = asyncio.Lock()
        self._migrate_done = False

    # --- 迁移控制 ---
    async def _check_migrate_needed(self) -> bool:
        """检查是否需要进行迁移。返回 True 表示需要迁移。"""
        try:
            # 读取迁移版本标记
            version = await self.plugin.get_kv_data("__migrate_version", 0)
            if int(version) >= self.MIGRATE_VERSION:
                # 已经迁移过了，不需要再迁移
                return False

            # 检查是否存在任何旧 key
            old_keys = [
                "balance:", "sub_expire:", "msg_count:",
                "__balance_registered:", "__sub_registered:",
                "__stats_balance_users", "__stats_sub_users"
            ]
            for prefix in old_keys:
                # 由于 KV 接口不支持 scan，我们只能检查特定的已知旧 key
                # 这里采用启发式：检查是否有旧统计 key 存在
                if prefix.startswith("__"):
                    val = await self.plugin.get_kv_data(prefix, None)
                    if val is not None:
                        return True
                # 对于用户级旧 key，我们无法预先知道所有 key，
                # 所以在 _get_with_fallback 中处理

            # 检查是否有旧统计 key
            old_stats = ["__stats_balance_users", "__stats_sub_users"]
            for key in old_stats:
                val = await self.plugin.get_kv_data(key, None)
                if val is not None:
                    return True

            # 没有发现旧 key，标记为已迁移（空迁移），下次不再检查
            await self.plugin.put_kv_data("__migrate_version", self.MIGRATE_VERSION)
            return False
        except Exception as e:
            logger.warning(f"[ChatChargeStorage] 迁移检查失败: {e}")
            return False

    async def _mark_migrate_done(self):
        """标记迁移完成"""
        try:
            await self.plugin.put_kv_data("__migrate_version", self.MIGRATE_VERSION)
            self._migrate_done = True
        except Exception as e:
            logger.warning(f"[ChatChargeStorage] 标记迁移完成失败: {e}")

    async def _get_with_fallback(self, new_key: str, old_key: str, default=0, type_cast=int):
        """先读新 key，没有则读旧 key 并迁移（带锁保护）"""
        # 快速路径：先读新 key
        try:
            val = await self.plugin.get_kv_data(new_key, default)
            # 如果新 key 存在且不是默认值，直接返回
            # 注意：这里无法 100% 区分"存了 default"和"没存"，
            # 所以还是要检查旧 key
        except Exception:
            val = default

        # 如果已经确认没有旧 key（空迁移），直接返回
        if self._migrate_done:
            return type_cast(val)

        # 检查是否需要迁移（只检查一次）
        if not self._migrate_done:
            needed = await self._check_migrate_needed()
            if not needed:
                self._migrate_done = True
                return type_cast(val)

        # 需要迁移，加锁保护
        async with self._migrate_lock:
            # 双重检查：可能其他协程已经迁移过了
            if self._migrate_done:
                # 重新读新 key
                try:
                    val = await self.plugin.get_kv_data(new_key, default)
                except Exception:
                    val = default
                return type_cast(val)

            # 尝试读旧 key
            try:
                old_val = await self.plugin.get_kv_data(old_key, None)
                if old_val is not None:
                    migrated = type_cast(old_val)
                    await self.plugin.put_kv_data(new_key, migrated)
                    try:
                        await self.plugin.delete_kv_data(old_key)
                    except Exception:
                        pass
                    return migrated
            except Exception:
                pass

            # 旧 key 不存在，返回新 key 的值
            return type_cast(val)

    async def _set(self, key: str, value):
        try:
            await self.plugin.put_kv_data(key, value)
        except Exception:
            pass

    async def _del(self, key: str):
        try:
            await self.plugin.delete_kv_data(key)
        except Exception:
            pass

    # ---------- 统一接口 ----------
    async def get_balance(self, scope_type: str, scope_id: str) -> int:
        if scope_type == "group":
            return await self._get_group_balance(scope_id)
        return await self._get_private_balance(scope_id)

    async def set_balance(self, scope_type: str, scope_id: str, amount: int):
        if scope_type == "group":
            await self._set_group_balance(scope_id, amount)
        else:
            await self._set_private_balance(scope_id, amount)

    async def get_expire(self, scope_type: str, scope_id: str) -> float:
        if scope_type == "group":
            return await self._get_group_expire(scope_id)
        return await self._get_private_expire(scope_id)

    async def set_expire(self, scope_type: str, scope_id: str, ts: float):
        if scope_type == "group":
            await self._set_group_expire(scope_id, ts)
        else:
            await self._set_private_expire(scope_id, ts)

    async def get_msg_count(self, scope_type: str, scope_id: str) -> int:
        if scope_type == "group":
            return await self._get_group_msg_count(scope_id)
        return await self._get_private_msg_count(scope_id)

    async def inc_msg_count(self, scope_type: str, scope_id: str):
        if scope_type == "group":
            await self._inc_group_msg_count(scope_id)
        else:
            await self._inc_private_msg_count(scope_id)

    async def mark_balance_user(self, scope_type: str, scope_id: str):
        if scope_type == "group":
            await self._mark_group_balance_user(scope_id)
        else:
            await self._mark_private_balance_user(scope_id)

    async def mark_sub_user(self, scope_type: str, scope_id: str):
        if scope_type == "group":
            await self._mark_group_sub_user(scope_id)
        else:
            await self._mark_private_sub_user(scope_id)

    # ---------- 个人存储（兼容旧 key） ----------
    async def get_private_balance(self, user_id: str) -> int:
        return await self._get_with_fallback(
            f"balance:private:{user_id}", f"balance:{user_id}", 0, int
        )

    async def _get_private_balance(self, user_id: str) -> int:
        return await self.get_private_balance(user_id)

    async def set_private_balance(self, user_id: str, amount: int):
        await self._set(f"balance:private:{user_id}", max(0, int(amount)))
        await self._del(f"balance:{user_id}")

    async def _set_private_balance(self, user_id: str, amount: int):
        await self.set_private_balance(user_id, amount)

    async def get_private_expire(self, user_id: str) -> float:
        return await self._get_with_fallback(
            f"sub_expire:private:{user_id}", f"sub_expire:{user_id}", 0.0, float
        )

    async def _get_private_expire(self, user_id: str) -> float:
        return await self.get_private_expire(user_id)

    async def set_private_expire(self, user_id: str, ts: float):
        await self._set(f"sub_expire:private:{user_id}", max(0.0, float(ts)))
        await self._del(f"sub_expire:{user_id}")

    async def _set_private_expire(self, user_id: str, ts: float):
        await self.set_private_expire(user_id, ts)

    async def get_private_msg_count(self, user_id: str) -> int:
        return await self._get_with_fallback(
            f"msg_count:private:{user_id}", f"msg_count:{user_id}", 0, int
        )

    async def _get_private_msg_count(self, user_id: str) -> int:
        return await self.get_private_msg_count(user_id)

    async def inc_private_msg_count(self, user_id: str):
        try:
            cnt = await self.get_private_msg_count(user_id)
            await self._set(f"msg_count:private:{user_id}", cnt + 1)
        except Exception:
            pass

    async def _inc_private_msg_count(self, user_id: str):
        await self.inc_private_msg_count(user_id)

    # ---------- 群存储 ----------
    async def get_group_balance(self, group_id: str) -> int:
        try:
            val = await self.plugin.get_kv_data(f"balance:group:{group_id}", 0)
            return max(0, int(val))
        except Exception:
            return 0

    async def _get_group_balance(self, group_id: str) -> int:
        return await self.get_group_balance(group_id)

    async def set_group_balance(self, group_id: str, amount: int):
        await self._set(f"balance:group:{group_id}", max(0, int(amount)))

    async def _set_group_balance(self, group_id: str, amount: int):
        await self.set_group_balance(group_id, amount)

    async def get_group_expire(self, group_id: str) -> float:
        try:
            val = await self.plugin.get_kv_data(f"sub_expire:group:{group_id}", 0.0)
            return max(0.0, float(val))
        except Exception:
            return 0.0

    async def _get_group_expire(self, group_id: str) -> float:
        return await self.get_group_expire(group_id)

    async def set_group_expire(self, group_id: str, ts: float):
        await self._set(f"sub_expire:group:{group_id}", max(0.0, float(ts)))

    async def _set_group_expire(self, group_id: str, ts: float):
        await self.set_group_expire(group_id, ts)

    async def get_group_msg_count(self, group_id: str) -> int:
        try:
            val = await self.plugin.get_kv_data(f"msg_count:group:{group_id}", 0)
            return max(0, int(val))
        except Exception:
            return 0

    async def _get_group_msg_count(self, group_id: str) -> int:
        return await self.get_group_msg_count(group_id)

    async def inc_group_msg_count(self, group_id: str):
        try:
            cnt = await self.get_group_msg_count(group_id)
            await self._set(f"msg_count:group:{group_id}", cnt + 1)
        except Exception:
            pass

    async def _inc_group_msg_count(self, group_id: str):
        await self.inc_group_msg_count(group_id)

    # ---------- 统计 ----------
    async def _mark_user(self, new_key: str, old_key, stats_key: str, old_stats_key=None):
        """通用标记逻辑，带兼容。old_key 和 old_stats_key 可为 None。"""
        try:
            existed = await self.plugin.get_kv_data(new_key, False)
            if existed:
                return

            # 兼容旧标记
            if old_key is not None:
                old_existed = await self.plugin.get_kv_data(old_key, False)
                if old_existed:
                    await self._set(new_key, True)
                    await self._del(old_key)
                    return

            # 新统计计数
            current = await self.plugin.get_kv_data(stats_key, 0)
            if not current and old_stats_key is not None:
                old_current = await self.plugin.get_kv_data(old_stats_key, 0)
                if old_current:
                    current = int(old_current)
                    await self._set(stats_key, current)
                    await self._del(old_stats_key)

            new_val = (int(current) if current else 0) + 1
            await self._set(stats_key, new_val)
            await self._set(new_key, True)
        except Exception:
            pass

    async def mark_private_balance_user(self, user_id: str):
        await self._mark_user(
            f"__registered:private:balance:{user_id}",
            f"__balance_registered:{user_id}",
            "__stats:private:balance_users",
            "__stats_balance_users"
        )

    async def _mark_private_balance_user(self, user_id: str):
        await self.mark_private_balance_user(user_id)

    async def mark_group_balance_user(self, group_id: str):
        await self._mark_user(
            f"__registered:group:balance:{group_id}",
            None,
            "__stats:group:balance_users"
        )

    async def _mark_group_balance_user(self, group_id: str):
        await self.mark_group_balance_user(group_id)

    async def mark_private_sub_user(self, user_id: str):
        await self._mark_user(
            f"__registered:private:sub:{user_id}",
            f"__sub_registered:{user_id}",
            "__stats:private:sub_users",
            "__stats_sub_users"
        )

    async def _mark_private_sub_user(self, user_id: str):
        await self.mark_private_sub_user(user_id)

    async def mark_group_sub_user(self, group_id: str):
        await self._mark_user(
            f"__registered:group:sub:{group_id}",
            None,
            "__stats:group:sub_users"
        )

    async def _mark_group_sub_user(self, group_id: str):
        await self.mark_group_sub_user(group_id)

    async def get_stats(self) -> dict:
        try:
            p_bc = await self.plugin.get_kv_data("__stats:private:balance_users", 0)
            p_sc = await self.plugin.get_kv_data("__stats:private:sub_users", 0)
            g_bc = await self.plugin.get_kv_data("__stats:group:balance_users", 0)
            g_sc = await self.plugin.get_kv_data("__stats:group:sub_users", 0)

            # 兼容旧统计
            if not p_bc:
                old = await self.plugin.get_kv_data("__stats_balance_users", 0)
                if old:
                    p_bc = int(old)
            if not p_sc:
                old = await self.plugin.get_kv_data("__stats_sub_users", 0)
                if old:
                    p_sc = int(old)

            return {
                "private_balance": int(p_bc) if p_bc is not None else 0,
                "private_sub": int(p_sc) if p_sc is not None else 0,
                "group_balance": int(g_bc) if g_bc is not None else 0,
                "group_sub": int(g_sc) if g_sc is not None else 0,
            }
        except Exception:
            return {
                "private_balance": 0,
                "private_sub": 0,
                "group_balance": 0,
                "group_sub": 0,
            }
