from app.casbin_new_func import FilterCondition
from app.db_mongo import filter_service

filter_data = FilterCondition("v0", "luoyu", "eq")
data = filter_service.filter_policies_advanced(conditions=[filter_data],include_inheritance=True)
