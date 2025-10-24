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
    """Casbin MongoDB 多字段过滤器 - 支持角色继承"""
    
    def __init__(self, mongo_uri: str, database_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        self.collection: Collection = self.db['casbin_rule']
    
    def build_query(self, conditions: List[FilterCondition]) -> Dict[str, Any]:
        """构建 MongoDB 查询条件"""
        query = {}
        
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
    
    def get_user_roles(self, user_id: str) -> List[str]:
        """
        获取用户的所有角色（包含继承）
        
        Args:
            user_id: 用户ID
        
        Returns:
            角色列表
        """
        # 查询 g 类型的记录，获取用户的直接角色
        roles = []
        
        # 第一步：获取用户直接分配的角色
        direct_roles = list(self.collection.find(
            {'ptype': 'g', 'v0': user_id},
            {'v1': 1, '_id': 0}
        ))
        
        for role_doc in direct_roles:
            role_name = role_doc.get('v1')
            if role_name not in roles:
                roles.append(role_name)
        
        # 第二步：递归获取角色的继承角色（role in role）
        visited = set(roles)
        queue = list(roles)
        
        while queue:
            current_role = queue.pop(0)
            
            # 查询这个角色继承的其他角色
            inherited_roles = list(self.collection.find(
                {'ptype': 'g', 'v0': current_role},
                {'v1': 1, '_id': 0}
            ))
            
            for role_doc in inherited_roles:
                inherited_role = role_doc.get('v1')
                if inherited_role not in visited:
                    roles.append(inherited_role)
                    visited.add(inherited_role)
                    queue.append(inherited_role)
        
        return roles
    
    def filter_policies(
        self, 
        conditions: List[FilterCondition],
        p_type: str = "p",
        include_inheritance: bool = False
    ) -> List[Dict[str, Any]]:
        """
        获取符合条件的 policies
        
        Args:
            conditions: 过滤条件列表
            p_type: policy 类型，通常为 "p" 或 "g"
            include_inheritance: 是否包含继承的权限（当查询用户权限时有效）
        
        Returns:
            匹配的 policy 列表
        """
        query = self.build_query(conditions)
        query['ptype'] = p_type
        
        results = list(self.collection.find(query, {'_id': 0}))
        
        # 如果需要包含继承且查询的是 p 类型，需要处理角色继承
        if include_inheritance and p_type == "p" and conditions:
            # 找到 v0 字段的条件（通常是用户或角色）
            v0_conditions = [c for c in conditions if c.field == 'v0']
            
            if v0_conditions:
                v0_value = v0_conditions[0].value
                
                # 获取用户的所有角色（包含继承）
                all_roles = self.get_user_roles(v0_value)
                
                if all_roles:
                    # 为每个角色查询权限
                    for role in all_roles:
                        # 构建新的查询条件，用角色替换 v0
                        role_conditions = [
                            c for c in conditions if c.field != 'v0'
                        ]
                        role_conditions.append(
                            FilterCondition(field='v0', value=role, operator='eq')
                        )
                        
                        role_query = self.build_query(role_conditions)
                        role_query['ptype'] = 'p'
                        
                        role_results = list(self.collection.find(role_query, {'_id': 0}))
                        results.extend(role_results)
        
        return results
    
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
        高级过滤接口（支持分页、排序和继承）
        
        Args:
            conditions: 过滤条件列表
            p_type: policy 类型
            skip: 跳过数量
            limit: 限制数量
            sort_by: 排序字段
            include_inheritance: 是否包含继承的权限
        
        Returns:
            包含总数和结果的字典
        """
        query = self.build_query(conditions)
        query['ptype'] = p_type
        
        # 获取总数
        total = self.collection.count_documents(query)
        
        # 构建查询
        cursor = self.collection.find(query, {'_id': 0})
        
        if sort_by:
            cursor = cursor.sort(sort_by, 1)
        
        results = list(cursor.skip(skip).limit(limit))
        
        # 如果需要包含继承且查询的是 p 类型
        if include_inheritance and p_type == "p" and conditions:
            v0_conditions = [c for c in conditions if c.field == 'v0']
            
            if v0_conditions:
                v0_value = v0_conditions[0].value
                all_roles = self.get_user_roles(v0_value)
                
                if all_roles:
                    # 重新计算总数（包含继承的权限）
                    all_results = []
                    
                    for role in all_roles:
                        role_conditions = [
                            c for c in conditions if c.field != 'v0'
                        ]
                        role_conditions.append(
                            FilterCondition(field='v0', value=role, operator='eq')
                        )
                        
                        role_query = self.build_query(role_conditions)
                        role_query['ptype'] = 'p'
                        
                        role_results = list(self.collection.find(role_query, {'_id': 0}))
                        all_results.extend(role_results)
                    
                    total = len(all_results)
                    # 应用分页
                    results = all_results[skip:skip + limit]
        
        return {
            'total': total,
            'skip': skip,
            'limit': limit,
            'data': results
        }
    
    def get_user_policies(
        self,
        user_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        获取用户的所有权限（包含继承的角色权限）
        
        Args:
            user_id: 用户ID
            skip: 分页 skip
            limit: 分页 limit
        
        Returns:
            用户的完整权限信息
        """
        # 获取用户的所有角色
        user_roles = self.get_user_roles(user_id)
        
        # 查询用户直接的权限
        query = {'ptype': 'p', 'v0': user_id}
        user_direct_policies = list(self.collection.find(query, {'_id': 0}))
        
        # 查询用户角色对应的权限
        all_policies = list(user_direct_policies)
        
        if user_roles:
            role_query = {'ptype': 'p', 'v0': {'$in': user_roles}}
            role_policies = list(self.collection.find(role_query, {'_id': 0}))
            all_policies.extend(role_policies)
        
        # 去重
        seen = set()
        unique_policies = []
        for policy in all_policies:
            policy_tuple = tuple(sorted(policy.items()))
            if policy_tuple not in seen:
                seen.add(policy_tuple)
                unique_policies.append(policy)
        
        total = len(unique_policies)
        paginated_policies = unique_policies[skip:skip + limit]
        
        return {
            'user_id': user_id,
            'roles': user_roles,
            'total': total,
            'skip': skip,
            'limit': limit,
            'data': paginated_policies
        }
