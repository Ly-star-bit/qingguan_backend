from datetime import datetime
import os
from pathlib import Path
import traceback
import urllib
import zipfile
from PyPDF2 import PdfMerger
import httpx
from loguru import logger
from rpa_tools.morelink_utils.morelink_api import MoreLinkClient
from io import BytesIO
import pandas as pd

def maitou_export_api( a_number_list: list, morelink_client:MoreLinkClient,clinet_name:str):
    try:
        need_fba_rename_client = ['通用客户']
        all_data = morelink_client.dahuodingdan_worknum_search_httpx(numberno=",".join(a_number_list), signtype="工作单号")

        all_maitou_data = []
        for row in all_data:
            maitou_data_single = {
                "operNo": row["operNo"],
                "dsid": row["DSID"],
                "type": 1,
                "proc": "client_add_BigWaybill_warehousereceipt",
                "TheCompany": row.get("TheCompany",""),
                "country": row.get("country_code") or "",
                "repottype": "pdf",
                "jdtype": row.get("jdtype",""),
                "EntrustType": row.get("EntrustType",""),
                "serialno": 0,
                "new_num": 0
            }
            all_maitou_data.append(maitou_data_single)

        payload = {"param": all_maitou_data}
        encoded_data = urllib.parse.urlencode(payload)
        response = morelink_client.httpx_client.post(url="https://morelink56.com/ReportView/batchshippingmark", content=encoded_data, timeout=httpx.Timeout(30.0, connect=5.0))
        print(response.json())
        if response.status_code == 200 and response.json()["success"]:
            logger.info(response.json())
            download_url = response.json()['msg']
            download_response = httpx.get(download_url, follow_redirects=True, timeout=httpx.Timeout(30.0, connect=5.0))
            if download_response.status_code == 200:
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                zip_file_path = rf"./唛头zip/唛头_{clinet_name}_{current_time}.zip"
                #确保目录存在
                os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)

                with open(zip_file_path, 'wb') as file:
                    file.write(download_response.content)
                logger.info(f"文件已成功下载到：{zip_file_path}")

                # 解压 ZIP 文件
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(rf"./唛头zip/唛头_{clinet_name}_{current_time}")

                # 获取解压后的 PDF 文件名称
                extracted_folder = rf"./唛头zip/唛头_{clinet_name}_{current_time}"
                pdf_files = [f for f in os.listdir(extracted_folder) if f.endswith('.pdf')]

                # 将pdf_list中的pdf文件合并成一个pdf文件
                merger = PdfMerger()

                # 确保输出目录存在
                output_path = os.path.join(extracted_folder, '合并唛头.pdf')

                # 合并所有PDF文件
                for pdf in pdf_files:
                    pdf_path = os.path.join(extracted_folder, pdf)  # Get full path to the PDF
                    if os.path.exists(pdf_path):  # Check with full path
                        merger.append(pdf_path)   # Append with full path

                # 保存合并后的PDF
                merger.write(output_path)
                merger.close()


                for pdf_file in pdf_files:
                    pdf_name = os.path.splitext(pdf_file)[0]
                    for row in all_data:
                        if row.get("sono") == pdf_name:
                            d_code = row['d_code'].replace("\\", "_").replace("/", "_")  # 将反斜杠和正斜杠都替换为下划线
                            if clinet_name in need_fba_rename_client:
                                new_pdf_name = rf"{row['operNo']}_{d_code}_{row['yjnum']}箱_{row['fbano']}_{pdf_file}"
                            else:
                                new_pdf_name = rf"{row['operNo']}_{d_code}_{row['yjnum']}箱_{pdf_file}"
                            try:
                                source_path = Path(extracted_folder) / pdf_file
                                target_path = Path(extracted_folder) / new_pdf_name
                                # 检查文件名长度并处理特殊字符
                                target_name = str(target_path.name)
                                if len(target_name) > 255:  # Windows文件名长度限制
                                    target_name = target_name[:255]
                                target_name = target_name.replace('\n', '_')  # 替换换行符为下划线
                                target_path = target_path.parent / target_name
                                source_path.rename(target_path)
                                logger.info(f"重命名文件：{pdf_file} -> {new_pdf_name}")
                            except FileNotFoundError as e:
                                logger.error(f"文件重命名失败，文件未找到: {traceback.format_exc()}")
                                
                                continue
                            except Exception as e:
                                logger.error(f"文件重命名失败，发生未知错误: {traceback.format_exc()}")
                                continue
                # 创建一个新的 ZIP 文件
                with zipfile.ZipFile(zip_file_path, 'w') as new_zip:
                    # 遍历解压目录中的所有文件
                    for root, dirs, files in os.walk(extracted_folder):
                        for file in files:
                            file_path = os.path.join(root, file)
                            # 将文件添加到新的 ZIP 文件中
                            new_zip.write(file_path, os.path.relpath(file_path, extracted_folder))
                return zip_file_path
            else:
                logger.info(f"下载失败，状态码：{download_response.status_code}")
    except Exception as e:
        logger.error(f"唛头下载失败，发生未知错误: {traceback.format_exc()}")
        return None


def fujian_maitou_export_api(morelink_client: MoreLinkClient, excel_content:bytes, clinet_name:str, is_binary=True):
    try:
        if is_binary:
            bytes_io = BytesIO(excel_content)
            df = pd.read_excel(bytes_io)
        else:
            df = pd.read_excel(excel_content)
        tihuo_date =  pd.to_datetime(df["提货日期"][0], unit="D", origin="1899-12-30").strftime("%m-%d")
        result = df.groupby('提货地址')['订单编号'].apply(list).reset_index()
        output = [{"提货地址":row['提货地址'],"all_fba_list":row['订单编号']} for index, row in result.iterrows()]

        filter_table_data = []
        all_data = morelink_client.dahuodingdan_all_data()
        for i in all_data:
            if i["customername"] == clinet_name and tihuo_date in str(i["DeliveryTime"]):
                filter_table_data.append(i)

        if not filter_table_data:
            logger.warning(f"没有找到客户 {clinet_name} 在提货日期 {tihuo_date} 的数据")
            return None

        all_maitou_data = []
        for row in filter_table_data:
            maitou_data_single = {
                    "operNo": row["operNo"],
                    "dsid": row["DSID"],
                    "type": 1,
                    "proc": "client_add_BigWaybill_warehousereceipt",
                    "TheCompany": row.get("TheCompany",""),
                    "country": row.get("country_code") or "",
                    "repottype": "pdf",
                    "jdtype": row.get("jdtype",""),
                    "EntrustType": row.get("EntrustType",""),
                    "serialno": 0,
                    "new_num": 0
                }
            all_maitou_data.append(maitou_data_single)

        payload = {"param":all_maitou_data}
        encoded_data = urllib.parse.urlencode(payload)
        response = morelink_client.httpx_client.post(url="https://morelink56.com/ReportView/batchshippingmark", content=encoded_data, timeout=httpx.Timeout(30.0, connect=5.0))

        if response.status_code == 200 and response.json()["success"]:
            logger.info(response.json())
            download_url = response.json()['msg']
            download_response = httpx.get(download_url, follow_redirects=True, timeout=httpx.Timeout(30.0, connect=5.0))
            if download_response.status_code == 200:
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                zip_dir_name = f"唛头_{clinet_name}_{current_time}"
                original_zip_path = Path(f"./唛头zip/{zip_dir_name}.zip")
                output_dir = Path(f'./唛头zip/{zip_dir_name}')
                
                os.makedirs(output_dir.parent, exist_ok=True)

                with open(original_zip_path, 'wb') as file:
                    file.write(download_response.content)
                logger.info(f"文件已成功下载到：{original_zip_path}")
                
                #处理so码和fba对应关系
                for entry in  output:
                    order_id_list = entry['all_fba_list']
                    all_sono = []
                    sono_fba_dict ={}
                    sono_yjnum_dict  = {}
                    for row in filter_table_data:
                        if row['fbano'] in order_id_list:
                            all_sono.append(row["sono"])
                            sono_fba_dict[row["sono"]] = row['fbano']
                            sono_yjnum_dict[row["sono"]] = row["yjnum"]

                    entry['all_sono'] = all_sono
                    entry["sono_fba_dict"] = sono_fba_dict
                    entry['sono_yjnum_dict'] = sono_yjnum_dict
                
                os.makedirs(output_dir, exist_ok=True)
                temp_extract_dir = Path(f'./唛头zip/temp_extract_{current_time}')
                temp_extract_dir.mkdir(exist_ok=True)
                with zipfile.ZipFile(original_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_extract_dir)

                try:
                    for entry in output:
                        address = entry["提货地址"]
                        sono_list = entry["all_sono"]
                        if not sono_list:
                            continue
                        sono_fba_dict = entry["sono_fba_dict"]
                        sono_yjnum_dict = entry["sono_yjnum_dict"]
                        zip_filename = f"{address}--箱唛个数为{len(sono_list)}条.zip"
                        
                        # 创建目录
                        zip_dir = Path(output_dir)
                        zip_dir.mkdir(parents=True, exist_ok=True)
                        # 处理文件名中的特殊字符
                        invalid_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t']
                        safe_filename = zip_filename
                        for char in invalid_chars:
                            safe_filename = safe_filename.replace(char, '_')
                        # 移除多余的下划线
                        safe_filename = '_'.join(filter(None, safe_filename.split('_')))
                        zip_path = zip_dir / safe_filename
                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for sono in sono_list:
                                sono_file_path = temp_extract_dir / f"{sono}.pdf"
                                if sono_file_path.exists():
                                    zipf.write(str(sono_file_path), arcname=f"{sono_fba_dict[sono]}-{sono_yjnum_dict[sono]}箱.pdf")
                                else:
                                    logger.warning(f"警告：{sono_file_path} 未找到，未添加到ZIP中。")
                        
                        logger.info(f"ZIP文件 {zip_path} 已创建。")
                    
                    # 将 output_dir 压缩成一个新的 zip 文件
                    final_zip_path = Path(f'./唛头zip/{zip_dir_name}_按地址合并.zip')
                    with zipfile.ZipFile(final_zip_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
                        for item in output_dir.rglob('*'):
                            if item.is_file():
                                new_zip.write(item, item.relative_to(output_dir))
                    
                    logger.info(f"最终的ZIP文件已创建于: {final_zip_path}")
                    return str(final_zip_path)

                
                finally:
                    import shutil
                    shutil.rmtree(temp_extract_dir)
                    shutil.rmtree(output_dir)
                    os.remove(original_zip_path)
                    
            else:
                logger.info(f"下载失败，状态码：{download_response.status_code}")
    except Exception as e:
        logger.error(f"福建唛头下载失败，发生未知错误: {traceback.format_exc()}")
        return None

