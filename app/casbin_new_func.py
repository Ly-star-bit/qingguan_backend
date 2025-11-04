from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from dataclasses import dataclass

@dataclass
class FilterCondition:
    """过滤条件"""
    field: str  # v0, v1, v2, v3, etc.
    value: Any
    operator: str = "eq"  # eq, contains, regex, in, gt, lt

class CasbinPolicyFilter:
    """Casbin MongoDB 多字段过滤器 - 支持角色继承和管理员权限"""

    def __init__(self, mongo_uri: str, database_name: str, admin_role: str = "admin_role"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
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
                query[condition.field] = {"$regex": condition.value, "$options": "i"}

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
        )
        return admin_role_doc is not None

    def get_user_roles(self, user_id: str) -> List[str]:
        """
        获取用户的所有角色（包含继承）
        """
        roles: List[str] = []

        # 第一步：获取用户直接分配的角色
        direct_roles = list(self.collection.find(
            {'ptype': 'g', 'v0': user_id},
            {'v1': 1, '_id': 0}
        ))
        for role_doc in direct_roles:
            role_name = role_doc.get('v1')
            if role_name and role_name not in roles:
                roles.append(role_name)

        # 第二步：递归获取角色的继承角色（role in role）
        visited = set(roles)
        queue = list(roles)

        while queue:
            current_role = queue.pop(0)
            inherited_roles = list(self.collection.find(
                {'ptype': 'g', 'v0': current_role},
                {'v1': 1, '_id': 0}
            ))
            for role_doc in inherited_roles:
                inherited_role = role_doc.get('v1')
                if inherited_role and inherited_role not in visited:
                    roles.append(inherited_role)
                    visited.add(inherited_role)
                    queue.append(inherited_role)

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
                    for role in all_roles:
                        role_conditions = [c for c in conditions if c.field != 'v0']
                        role_conditions.append(FilterCondition(field='v0', value=role, operator='eq'))
                        role_query = self.build_query(role_conditions)
                        role_query['ptype'] = 'p'
                        role_results = list(self.collection.find(role_query, {'_id': 0}))
                        results.extend(role_results)

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
          保持原有行为；当 include_inheritance=True 时，合并其角色（含继承）的 p 记录；
          再按 sort_by 排序（原始字段），最后分页。
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

                # 非管理员：合并角色权限
                all_roles = self.get_user_roles(user_id)
                all_results: List[Dict[str, Any]] = []
                for role in all_roles:
                    role_conditions = [c for c in conditions if c.field != 'v0']
                    role_conditions.append(FilterCondition(field='v0', value=role, operator='eq'))
                    role_query = self.build_query(role_conditions)
                    role_query['ptype'] = 'p'
                    role_results = list(self.collection.find(role_query, {'_id': 0}))
                    all_results.extend(role_results)

                total = len(all_results)
                if sort_by:
                    all_results.sort(key=lambda x: x.get(sort_by, ''))
                results = all_results[skip:skip + limit]
                return {
                    'total': total,
                    'skip': skip,
                    'limit': limit,
                    'data': results
                }

        # 非继承或非 p 类型：沿用原分页逻辑
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
