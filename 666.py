from app.db_mongo import filter_service, enforcer

# # 获取所有的policy
# all_policies = enforcer.get_policy()

# # 遍历所有policy并修复obj路径
# for i, policy in enumerate(all_policies):
#     if len(policy) >= 2:
#         obj = policy[1]
        
#         # 检查obj是否需要添加后缀
#         if len(obj) > 1 and not obj.endswith('/'):
#             # 创建新的policy
#             new_policy = list(policy)
#             new_policy[1] = obj + '/'
            
#             # 删除旧policy并添加新policy
#             enforcer.remove_policy(*policy)
#             enforcer.add_policy(*new_policy)
            
#             print(f"Updated policy: {policy} -> {new_policy}")

# 测试enforce
a = enforcer.enforce("air_china2usa_qingguan", "/qingguan/api/exchange-rate/", "GET", {})

print(123)
print(f"Enforce result: {a}")
