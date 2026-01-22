from datetime import datetime, timedelta
from app.tools_auto.utils.hubs.hubs_client import HubsClient


hubs_client = HubsClient()
today = datetime.now()
one_month_ago = today - timedelta(days=30)
s_date = one_month_ago.strftime("%Y-%m-%d")
e_date = today.strftime("%Y-%m-%d")
payload = {"pageindex":1,"pagesize":200,"open_search":False,"sorts":[],"s_date":s_date,"e_date":e_date,"transport_type":"海运","sel_type":"operno","ids":[],"searchlist":[]}
hubs_zongdan_data_res = hubs_client.httpx_client.post("https://admin.hubs-scs.com/api/v1/oms/oms_zginfo/getlist",json=payload)
hubs_zongdan_data = hubs_zongdan_data_res.json()
filter_data = [i for i in hubs_zongdan_data['data']['list'] if i['bl_no'] == 'EGLV143661443206']

field_mapping = {
    "hbl_no": "hblno",
    "pol": "startland",
    "pod2": "destination",
    "vsl_voy": "flight",
    "cabinet_type": "cabinetNo",
    "seal_no": "sealno",
    "atd": "ATD",
}
mapped_data = []
for item in filter_data:
    mapped_item = {field_mapping.get(k, k): v for k, v in item.items()}
    mapped_data.append(mapped_item)

print(mapped_data)
