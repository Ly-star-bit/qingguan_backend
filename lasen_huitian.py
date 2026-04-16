
from io import BytesIO

import pandas as pd

from app.tools_auto.utils.hubs.hubs_client import HubsClient

from rpa_tools.email_tools import send_email,read_email_by_subject

def safe_float(value):
    try:
        if value == "" or value is None:
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def format_zip_code(zip_code):
    try:
        if zip_code is None or zip_code == "":
            return ""
        
        zip_str = str(zip_code).strip()
        
        # 处理 ZIP+4 格式（如 12345-6789）
        if '-' in zip_str:
            parts = zip_str.split('-')
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                return f"{parts[0].zfill(5)}-{parts[1].zfill(4)}"
        
        # 处理标准5位邮编
        if zip_str.isdigit():
            return zip_str.zfill(5)
        
        # 其他数字形式
        return str(int(float(zip_str))).zfill(5)
    
    except (ValueError, TypeError):
        return ""


def convert_search_code(origin_d_code: str, all_fbx_address: list) -> dict:
    origin_d_code = str(origin_d_code).strip()

    if origin_d_code.isdigit():
        if not origin_d_code.startswith("0") and len(origin_d_code) != 5:
            origin_d_code = "0" + origin_d_code
        new_d_code = origin_d_code
    else:
        new_d_code = origin_d_code

    matched_warehouses = []
    for single_fbx_warehouse in all_fbx_address:
        if (
            new_d_code == single_fbx_warehouse["d_code"]
            and "暂停使用" not in single_fbx_warehouse["d_code"]
        ):
            matched_warehouses.append(single_fbx_warehouse)

    result = {
        "d_code": new_d_code,
        "fbx_name": "亚马逊",
        "fbx_code": "",
        "sh_addr": "",
        "sh_zip_code": "",
        "sh_province": "",
        "sh_city": "",
        "sh_email": "",
        "country_name": "美国",
        "country_code": "US",
        "addr_type": 0,
        "found": False,
    }

    if len(matched_warehouses) == 1:
        matched = matched_warehouses[0]
        result["d_code"] = matched.get("d_code", "") or ""
        result["fbx_name"] = matched.get("fbx_name", "") or ""
        result["fbx_code"] = matched.get("fbx_code", "") or ""
        result["sh_addr"] = matched.get("addr", "") or ""
        result["sh_zip_code"] = format_zip_code(matched.get("zip_code"))
        result["sh_province"] = matched.get("zhou", "") or ""
        result["sh_city"] = matched.get("city", "") or ""
        result["sh_email"] = matched.get("email", "") or ""
        result["country_name"] = matched.get("country_name", "") or "美国"
        result["country_code"] = matched.get("country_code", "") or "US"
        result["found"] = True
    elif len(matched_warehouses) > 1:
        result["d_code"] = "TBD"
        result["fbx_name"] = "TBD"
        result["fbx_code"] = "TBD"
        result["addr_type"] = 2
        result["found"] = False
    else:
        result["d_code"] = "TBD"
        result["fbx_name"] = "TBD"
        result["fbx_code"] = "TBD"
        result["addr_type"] = 2
        result["found"] = False

    return result


def generate_boxgauge_list(fbano: str, yj_qty: int, po_number: str = "") -> list:
    """根据 fba_no 和 yj_qty 生成箱清单列表"""
    fbano_list = [f.strip() for f in fbano.split(",") if f.strip()]
    
    if len(fbano_list) == 1:
        single_fbano = fbano_list[0]
        if "FBA" in single_fbano.upper():
            return [
                {
                    "bgid": "",
                    "fba_no": single_fbano,
                    "container_no": f"{single_fbano}U{i:06d}",
                    "tracking_no": "",
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "qty": 1,
                    "build_date": "",
                    "singlebox_kg": 0,
                    "refid": po_number or "",
                }
                for i in range(1, int(yj_qty) + 1)
            ]
        else:
            return [
                {
                    "bgid": "",
                    "fba_no": single_fbano,
                    "container_no": single_fbano,
                    "tracking_no": "",
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "qty": int(yj_qty),
                    "build_date": "",
                    "singlebox_kg": 0,
                    "refid": po_number or "",
                }
            ]
    return []


def process_excel_to_json2(excel_content, is_binary=True, all_fbx_address: list = None, po_number: str = ""):
    if is_binary:
        bytes_io = BytesIO(excel_content)
        df = pd.read_excel(bytes_io)
    else:
        df = pd.read_excel(excel_content)
    df = df[~df.isin(df.columns).all(axis=1)]
    df = df[df["仓库"].notna()]
    df.columns = df.columns.str.strip()
    df["仓库代码"] = df["仓库"].astype(str).str.strip()
    df["订单单号"] = df["发货批次"].astype(str)
    df["数量"] = df["箱数"]
    df["重量"] = df["总实重"]
    df = df.dropna(how="all")

    df["进仓号"] = df["进仓号"].ffill()
    grouped = df.groupby("进仓号")

    result_list = []

    for name, group in grouped:
        fba_no = ",".join(group["订单单号"].unique())
        yj_qty = int(safe_float(group["数量"].sum()))
        yj_kg = round(safe_float(group["重量"].sum()), 2)
        yj_cbm = round(safe_float(group["总箱数"].sum()), 3)
        a_number = group["进仓号"].dropna().unique()[0]
        take_goods = "\n".join(map(str, group["总箱数"].dropna().unique()))
        origin_code = group["仓库代码"].dropna().unique()[0]

        converted = {"d_code": origin_code, "found": False}
        if all_fbx_address:
            converted = convert_search_code(origin_code, all_fbx_address)

       
        current_boxgauge = []
        for sub_fba, sub_df in group.groupby("订单单号"):
            sub_fba = str(sub_fba).strip()
            if not sub_fba:
                continue
            sub_qty = int(safe_float(sub_df["数量"].sum()))
            current_boxgauge.extend(generate_boxgauge_list(sub_fba, sub_qty, po_number))

        result = {
            "order": {
                "fbx_no": fba_no,
                "yj_qty": yj_qty,
                "yj_kg": yj_kg,
                "yj_cbm":yj_cbm,
                "have_no": a_number,
                "packing": "箱",
                "d_code": converted["d_code"],
                "fbx_name": converted.get("fbx_name", ""),
                "fbx_code": converted.get("fbx_code", ""),
                "sh_addr": converted.get("sh_addr", ""),
                "sh_zip_code": converted.get("sh_zip_code", ""),
                "sh_province": converted.get("sh_province", ""),
                "sh_city": converted.get("sh_city", ""),
                "sh_email": converted.get("sh_email", ""),
                "country_name": converted.get("country_name", "美国"),
                "country_code": converted.get("country_code", "US"),
                "addr_type": converted.get("addr_type", 0),
            },
            "boxgauge_list": current_boxgauge,
        }

        result_list.append(result)

    return result_list

def main():
    import traceback
    from loguru import logger
    logger.add("logs/lasen_huitian_{time:%Y%m%d}.log", rotation="00:00", retention="7 days", encoding="utf-8")

    subject_input = "lasen回填"
    logger.info(f"开始处理主题为 [{subject_input}] 的邮件")

    try:
        email_data = read_email_by_subject(subject_input=subject_input, seen=True)
        if not email_data:
            logger.warning("未找到符合条件的邮件")
     
            return

        logger.info(f"成功读取 {len(email_data)} 封邮件")

        hubs_client = HubsClient()
        all_fbx_address = hubs_client.get_base_fbx_addr()
        logger.info(f"获取到 {len(all_fbx_address)} 个仓库地址")

        success_count = 0
        fail_count = 0
        results_detail = []

        for idx, email_item in enumerate(email_data, 1):
            sender_email = email_item.get('from', '')
            try:
                logger.info(f"处理第 {idx} 封邮件，来自: {sender_email}")
                excel_attachment = None
                for att in email_item.get('attachments', []):
                    if att['filename'] and att['filename'].lower().endswith(('.xlsx', '.xls')):
                        excel_attachment = att
                        logger.info(f"找到Excel附件: {att['filename']}")
                        break

                if not excel_attachment:
                    logger.warning(f"第 {idx} 封邮件未找到Excel附件")
                    fail_count += 1
                    results_detail.append(f"第{idx}封邮件: 失败 - 未找到Excel附件")
                    send_email(
                        receiver_email=sender_email,
                        subject=f"[lasen回填]处理结果 - 第{idx}封",
                        body=f"处理失败: 未找到Excel附件\n\n发件人: {sender_email}"
                    )
                    continue

                data_list = process_excel_to_json2(
                    excel_attachment['content'],
                    is_binary=True,
                    all_fbx_address=all_fbx_address,
                )

                if not data_list:
                    logger.warning(f"第 {idx} 封邮件: Excel数据解析为空")
                    fail_count += 1
                    results_detail.append(f"第{idx}封邮件: 失败 - Excel数据解析为空")
                    send_email(
                        receiver_email=sender_email,
                        subject=f"[lasen回填]处理结果 - 第{idx}封",
                        body=f"处理失败: Excel数据解析为空\n\n发件人: {sender_email}"
                    )
                    continue

                success_list = []
                fail_list = []

                for data_idx, data in enumerate(data_list):
                    have_no = data.get('order', {}).get('have_no')
                    if not have_no:
                        logger.warning(f"第 {idx} 封邮件第 {data_idx+1} 条: have_no为空，跳过")
                        fail_list.append({"have_no": "空", "reason": "have_no为空"})
                        fail_count += 1
                        continue

                    logger.info(f"第 {idx} 封邮件第 {data_idx+1} 条: 开始回填进仓号 {have_no}")

                    result = hubs_client.update_bigwaybill_order([have_no], data)
                    logger.info(f"回填结果: {result}")

                    if result and result.get("success"):
                        logger.info(f"回填成功: {have_no}")
                        success_count += 1
                        success_list.append(have_no)
                    else:
                        error_msg = result.get("message", "未知错误") if result else "返回结果为空"
                        logger.warning(f"回填失败: {have_no}, 原因: {error_msg}")
                        fail_count += 1
                        fail_list.append({"have_no": have_no, "reason": error_msg})

                success_text = "\n".join(success_list) if success_list else "无"
                fail_text = "\n".join([f"{item['have_no']}: {item['reason']}" for item in fail_list]) if fail_list else "无"

                body = f"""处理完成

成功列表 ({len(success_list)} 条):
{success_text}

失败列表 ({len(fail_list)} 条):
{fail_text}

发件人: {sender_email}"""

                if len(fail_list) == 0:
                    subject = f"[lasen回填]处理结果 - 成功"
                elif len(success_list) == 0:
                    subject = f"[lasen回填]处理结果 - 全部失败"
                else:
                    subject = f"[lasen回填]处理结果 - 部分失败"

                send_email(
                    receiver_email=sender_email,
                    subject=subject,
                    body=body
                )

            except Exception as e:
                logger.error(f"处理第 {idx} 封邮件失败: {str(e)}")
                logger.error(traceback.format_exc())
                fail_count += 1
                results_detail.append(f"第{idx}封邮件: 失败 - {str(e)}")
                send_email(
                    receiver_email=sender_email,
                    subject=f"[lasen回填]处理结果 - 失败",
                    body=f"处理失败: {str(e)}\n\n发件人: {sender_email}\n\n详情:\n{traceback.format_exc()}"
                )

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        logger.error(traceback.format_exc())
        send_email(
            subject=f"[lasen回填]处理异常",
            body=f"程序执行异常: {str(e)}\n\n{traceback.format_exc()}"
        )


def run_scheduler(interval_seconds=180):
    import schedule
    import time as time_module
    from loguru import logger

    logger.info(f"启动定时任务，每 {interval_seconds} 秒执行一次")

    schedule.every(interval_seconds).seconds.do(main)

    while True:
        schedule.run_pending()
        time_module.sleep(1)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 180
        run_scheduler(interval)
    else:
        main()
    # hubs_client = HubsClient()
    # all_fbx_address = hubs_client.get_base_fbx_addr(pagesize=999)
    # process_excel_to_json2(is_binary=False, excel_content=r"C:\Users\a1337\Desktop\数据汇总-赫泊斯美空-1.30.xlsx",all_fbx_address=all_fbx_address)
