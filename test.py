from app.db_mongo import get_session
for db in get_session():
    result = db.products_sea.find_one({"中文品名": "N3-相机配件-摄影用","startland":"China"})
    break 
