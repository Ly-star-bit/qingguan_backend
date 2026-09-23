import re
from typing import List, Dict, Any, Optional
from pymongo.collection import Collection
from pymongo.database import Database
from dataclasses import dataclass

@dataclass
class FilterCondition:
    """过滤条件"""
    field: str  # v0, v1, v2, v3, etc.
    value: Any
    operator: str = "eq"  # eq, contains, regex, in, gt, lt

class CasbinPolicyFilter:
    """Casbin MongoDB 多字段过滤器 - 支持角色继承和管理员权限"""

    def __init__(self, database: Database, admin_role: str = "admin_role"):
        # 复用 db_mongo 中已建立的连接，不再单独创建 MongoClient
        self.db = database
        self.collection: Collection = self.db['casbin_rule']
        self.admin_role = admin_role

    # -----------------------------
    # 工具方法
    # -----------------------------
    def build_query(self, conditions: List[FilterCondition]) -> Dict[str, Any]:
        """构建 MongoDB 查询条件"""
        query: Dict[str, Any] = {}

        for condition in conditions:
            if condition.operator == "eq":
                query[condition.field] = condition.value

            elif condition.operator == "contains":
                # 按字面量包含匹配，避免用户输入被当作正则
                query[condition.field] = {"$regex": re.escape(str(condition.value)), "$options": "i"}

            elif condition.operator == "regex":
                query[condition.field] = {"$regex": condition.value}

            elif condition.operator == "in":
                query[condition.field] = {"$in": condition.value}

            elif condition.operator == "gt":
                query[condition.field] = {"$gt": condition.value}

            elif condition.operator == "lt":
                query[condition.field] = {"$lt": condition.value}

        return query

    def _build_query_excluding(self, conditions: List[FilterCondition], exclude_fields: List[str]) -> Dict[str, Any]:
        """基于 build_query，但排除某些字段（例如 v0）"""
        kept = [c for c in conditions if c.field not in exclude_fields]
        return self.build_query(kept) if kept else {}

    def _roles_query(self, conditions: List[FilterCondition], roles: List[str]) -> Dict[str, Any]:
        """其余条件不变，v0 替换为“属于这些角色之一”，一次查询取回所有角色的策略"""
        query = self._build_query_excluding(conditions, exclude_fields=['v0'])
        query['v0'] = {'$in': roles}
        query['ptype'] = 'p'
        return query

    def _collapse_to_unique_v1(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将文档列表折叠为唯一的 v1 列表，并将 v3 固定为 []（表示“所有属性”）
        输出形如： [{'v1': <接口>, 'v3': []}, ...]
        """
        seen = set()
        out: List[Dict[str, Any]] = []
        for d in docs:
            v1 = d.get('v1')
            if v1 is not None and v1 not in seen:
                seen.add(v1)
                out.append({'v1': v1, 'v3': '[]','v4':'allow'})
        return out

    # -----------------------------
    # 基础角色/管理员判断
    # -----------------------------
    def is_admin(self, user_id: str) -> bool:
        """
        检查用户是否是管理员（是否拥有 admin_role）
        """
        admin_role_doc = self.collection.find_one(
            {'ptype': 'g', 'v0': user_id, 'v1': self.admin_role},
            {'_id': 1},
        )
        return admin_role_doc is not None

    def get_user_roles(self, user_id: str) -> List[str]:
        """
        获取用户的所有角色（包含继承）。
        按层级批量查询：每一层只查一次（原来每个角色查一次）。
        """
        roles: List[str] = []
        visited = {user_id}
        level = [user_id]

        while level:
            next_level: List[str] = []
            for doc in self.collection.find({'ptype': 'g', 'v0': {'$in': level}}, {'v1': 1, '_id': 0}):
                role_name = doc.get('v1')
                if role_name and role_name not in visited:
                    visited.add(role_name)
                    roles.append(role_name)
                    next_level.append(role_name)
            level = next_level

        return roles

    # -----------------------------
    # 过滤（简版）
    # -----------------------------
    def filter_policies(
        self,
        conditions: List[FilterCondition],
        p_type: str = "p",
        include_inheritance: bool = False
    ) -> List[Dict[str, Any]]:
        """
        获取符合条件的 policies

        - 管理员(admin_role)：
          忽略 v0，仅按其余条件过滤 p 记录；
          最终只返回唯一 v1 的列表，且每项 v3=[]。

        - 非管理员：
          保持原有行为；当 include_inheritance=True 时，
          还会合并其角色（含继承）的 p 记录。
        """
        query = self.build_query(conditions)
        query['ptype'] = p_type

        results = list(self.collection.find(query, {'_id': 0}))

        # 管理员分支：仅在查询 p 且包含继承时启用（与原有结构一致）
        if include_inheritance and p_type == "p" and conditions:
            v0_conditions = [c for c in conditions if c.field == 'v0']
            if v0_conditions:
                user_id = v0_conditions[0].value
                # 命中管理员：忽略 v0，按其他条件过滤后折叠为唯一 v1，v3=[]
                if self.is_admin(user_id):
                    admin_query = self._build_query_excluding(conditions, exclude_fields=['v0'])
                    admin_query['ptype'] = 'p'
                    raw = list(self.collection.find(admin_query, {'_id': 0}))
                    return self._collapse_to_unique_v1(raw)

                # 非管理员：沿用原逻辑（用户角色及继承）
                all_roles = self.get_user_roles(user_id)
                if all_roles:
                    results.extend(self.collection.find(self._roles_query(conditions, all_roles), {'_id': 0}))

        return results

    # -----------------------------
    # 过滤（高级：分页/排序）
    # -----------------------------
    def filter_policies_advanced(
        self,
        conditions: List[FilterCondition],
        p_type: str = "p",
        skip: int = 0,
        limit: int = 100,
        sort_by: Optional[str] = None,
        include_inheritance: bool = False
    ) -> Dict[str, Any]:
        """
        高级过滤接口（支持分页、排序、继承和管理员权限）

        - 管理员(admin_role)：
          忽略 v0，仅按其余条件过滤 p 记录；
          再将结果折叠为唯一 v1（每项 v3=[]），
          然后对折叠结果按 v1 排序（如果 sort_by == 'v1'），再分页。

        - 非管理员：
          当 include_inheritance=True 时，返回其角色（含继承）的 p 记录
          （与原有行为一致：不包含直接挂在用户名下的 p 记录）；
          排序、分页在数据库端完成。
        """
        query = self.build_query(conditions)
        query['ptype'] = p_type

        if include_inheritance and p_type == "p" and conditions:
            v0_conditions = [c for c in conditions if c.field == 'v0']
            if v0_conditions:
                user_id = v0_conditions[0].value

                # 管理员：忽略 v0，按其他条件过滤 -> 折叠唯一 v1 -> 排序/分页
                if self.is_admin(user_id):
                    admin_query = self._build_query_excluding(conditions, exclude_fields=['v0'])
                    admin_query['ptype'] = 'p'
                    raw = list(self.collection.find(admin_query, {'_id': 0}))

                    collapsed = self._collapse_to_unique_v1(raw)
                    # 仅对 v1 排序有意义
                    if sort_by == 'v1':
                        collapsed.sort(key=lambda x: x['v1'])

                    total = len(collapsed)
                    data = collapsed[skip: skip + limit]
                    return {
                        'total': total,
                        'skip': skip,
                        'limit': limit,
                        'data': data
                    }

                # 非管理员：一次查询取回所有角色的策略
                all_roles = self.get_user_roles(user_id)
                if not all_roles:
                    return {'total': 0, 'skip': skip, 'limit': limit, 'data': []}
                query = self._roles_query(conditions, all_roles)

        total = self.collection.count_documents(query)
        cursor = self.collection.find(query, {'_id': 0})
        if sort_by:
            cursor = cursor.sort(sort_by, 1)
        results = list(cursor.skip(skip).limit(limit))
        return {
            'total': total,
            'skip': skip,
            'limit': limit,
            'data': results
        }
