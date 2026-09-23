"""
支持“禁用角色”的 Casbin 角色管理器。

角色的启用/禁用状态存在 roles 集合（status=0 为禁用），Casbin 本身不认识这个字段。
这里在角色继承关系上把被禁用的角色当作不存在：
- 鉴权：matcher 里的 g(用户, 角色) 不会经过被禁用的角色，
  用户既拿不到该角色的规则，也拿不到通过该角色继承来的上级角色的规则；
- get_implicit_roles_for_user / get_implicit_permissions_for_user 同样跳过被禁用的角色，
  所以菜单、权限项、admin 判断都会跟着生效。
g 规则本身不删不改（管理界面读取的是原始 g 规则），重新启用后立即恢复。
"""
from typing import Iterable

from casbin.rbac.default_role_manager import RoleManager


class StatusAwareRoleManager(RoleManager):
    def __init__(self, max_hierarchy_level: int = 10):
        super().__init__(max_hierarchy_level)
        self.disabled_roles: set = set()

    def set_disabled_roles(self, names: Iterable[str]) -> None:
        self.disabled_roles = {n for n in names if n}

    def _has_link(self, name, roles, level):
        if level <= 0 or len(roles) == 0:
            return False

        next_roles = set()
        for role in roles:
            if name == role.name:
                return True
            next_roles.update(r for r in role.roles if r.name not in self.disabled_roles)

        return self._has_link(name, list(next_roles), level - 1)

    def get_roles(self, name, *domain):
        return [r for r in super().get_roles(name, *domain) if r not in self.disabled_roles]
