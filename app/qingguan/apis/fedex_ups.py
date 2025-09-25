import io
import os
import re
from datetime import datetime
import PyPDF2
import numpy as np
import pandas as pd

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    UploadFile
)
from fastapi.responses import FileResponse, StreamingResponse

from app.utils import (
    extract_zip_codes_from_excel,
    get_ups_zip_data,
    fedex_process_excel_with_zip_codes,
    ups_process_excel_with_zip_codes,
)


express_delivery_router = APIRouter(
    prefix="/express_delivery",
    tags=["Express Delivery"],
)
@express_delivery_router.post("/process_excel_usp_data", summary="处理USPS报价Excel并生成结果")
async def process_excel_usp_data(file: UploadFile = File(...)):
    """处理上传的Excel文件"""
    try:
        # 读取上传的文件内容

        contents = await file.read()

        # 使用pandas读取Excel文件
        xls = pd.ExcelFile(io.BytesIO(contents))

        # 检查必要的工作表是否存在
        required_sheets = ["数据粘贴", "LAX分区", "燃油", "尾程25年非旺季报价单"]
        missing_sheets = [
            sheet for sheet in required_sheets if sheet not in xls.sheet_names
        ]

        if missing_sheets:
            raise ValueError(f"缺少工作表: {', '.join(missing_sheets)}")

        # 读取各个工作表
        sheet_data = pd.read_excel(xls, sheet_name="数据粘贴", header=1)
        if "邮编" in sheet_data.columns:
            sheet_data["邮编"] = sheet_data["邮编"].astype(str).str.zfill(5)
        sheet_lax_partition = pd.read_excel(xls, sheet_name="LAX分区", skiprows=5)
        sheet_fuel = pd.read_excel(xls, sheet_name="燃油")
        sheet_fuel = sheet_fuel.dropna(subset=[sheet_fuel.columns[0]])

        # sheet_usp_raw = pd.read_excel(xls, sheet_name='USPS报价单',skiprows=1)
        sheet_usp_25 = pd.read_excel(xls, sheet_name="尾程25年非旺季报价单", skiprows=1)

        # 检查数据有效性
        if sheet_data.empty or sheet_lax_partition.empty or sheet_fuel.empty:
            raise ValueError("一个或多个工作表为空")

        # 处理燃油数据
        fuel_data = []
        for _, row in sheet_fuel.iterrows():
            date_range = str(row[0]).split("~")
            start_date = date_range[0].strip()
            end_date = date_range[1].strip()

            # 将日-月-年转换为年-月-日
            start_parts = start_date.split("-")
            end_parts = end_date.split("-")

            if len(start_parts) == 3 and len(end_parts) == 3:
                try:
                    start_date = pd.to_datetime(
                        f"{start_parts[2]}-{start_parts[1]}-{start_parts[0]}",
                        format="%Y-%m-%d",
                    )
                    end_date = pd.to_datetime(
                        f"{end_parts[2]}-{end_parts[1]}-{end_parts[0]}",
                        format="%Y-%m-%d",
                    )
                except ValueError:
                    print(f"燃油数据日期转换错误: {start_date}, {end_date}")
                    continue

                fuel_data.append(
                    {
                        "startDate": start_date,
                        "endDate": end_date,
                        "rate": float(row[1]),
                    }
                )

        # 处理USPS报价单数据
        def process_usp_sheet(sheet):
            usp_data = {}
            for i in range(len(sheet)):
                row_name = sheet.iloc[i, 0]  # A列作为行名
                if pd.notna(row_name):
                    usp_data[row_name] = {}
                    for j in range(1, len(sheet.columns)):
                        col_name = sheet.columns[j]
                        try:
                            usp_data[row_name][col_name] = float(sheet.iloc[i, j])
                        except ValueError:
                            usp_data[row_name][col_name] = sheet.iloc[i, j]
            return usp_data

        # sheet_usp = process_usp_sheet(sheet_usp_raw)
        # sheet_usp_25_data = process_usp_sheet(sheet_usp_25)

        # 检查日期格式
        # sheet_data['第一枪\n扫描时间时间'] = pd.to_datetime(sheet_data['第一枪\n扫描时间时间'], errors='coerce')
        sheet_data["第一枪\n扫描时间时间"] = sheet_data["第一枪\n扫描时间时间"].apply(
            lambda x: pd.to_datetime("1899-12-30") + pd.to_timedelta(x, unit="D")
            if pd.notna(x) and isinstance(x, (int, float))
            else x
        )
        invalid_dates = sheet_data[sheet_data["第一枪\n扫描时间时间"].isna()]

        if not invalid_dates.empty:
            raise ValueError("日期格式不对可能为空")

        # # 获取联邦快递的邮政编码
        # fedex_pdf_path = os.path.join(
        #     os.getcwd(),
        #     "file",
        #     "remoteaddresscheck",
        #     "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
        # )  # 确保PDF文件名正确
        # fedex_zip_codes_by_category = extract_zip_codes_from_pdf(fedex_pdf_path)
        fedex_excel_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
        )
        fedex_zip_codes_by_category = extract_zip_codes_from_excel(fedex_excel_path)

        ups_zip_data = get_ups_zip_data()
        # 处理每一行数据
        for index, row in sheet_data.iterrows():
            # 计算计费重量
            jifei_weight = np.ceil(row["重量\n(LB)"])
            if pd.isna(jifei_weight):
                jifei_weight = 0

            # 获取邮编前五位
            zip_code = str(row["邮编"]).zfill(5)
            zip_code_prefix = zip_code[:5] if len(zip_code) >= 5 else zip_code

            # 查找分区
            partition = "未找到分区"
            for _, partition_row in sheet_lax_partition.iterrows():
                dest_zip = str(partition_row["Dest. ZIP"]).strip()
                if "-" in dest_zip:
                    zip_range = dest_zip.split("-")
                    if len(zip_range) == 2:
                        try:
                            start_zip = int(zip_range[0])
                            end_zip = int(zip_range[1])
                            zip_prefix = int(zip_code_prefix)
                            if start_zip <= zip_prefix <= end_zip:
                                partition = partition_row["Ground"]
                                break  # 找到分区后退出循环
                        except ValueError:
                            continue  # 如果转换失败，则跳过此行
                else:
                    if dest_zip.startswith(zip_code_prefix):
                        partition = partition_row["Ground"]
                        break  # 找到分区后退出循环

            # 获取订单日期
            order_date = row["第一枪\n扫描时间时间"]

            # 查找燃油费率
            fuel_rate = 0
            for fuel in fuel_data:
                if order_date and fuel["startDate"] <= order_date <= fuel["endDate"]:
                    fuel_rate = fuel["rate"]
                    break

            # 根据月份选择不同的报价单
            # current_sheet_usp = sheet_usp_25 #默认使用25年
            # 只需要到71行的数据
            current_sheet_usp = sheet_usp_25.iloc[:50]

            # 查找价格
            money = 0
            if int(jifei_weight) in [
                int(i) for i in current_sheet_usp["Ibs"].values
            ] and int(partition) in [
                int(i) for i in list(current_sheet_usp.columns)[2:]
            ]:
                partition = str(int(float(partition))).zfill(3)
                money = current_sheet_usp.loc[int(jifei_weight) - 1, partition]

            # 计算总金额
            all_money = np.ceil(money * (1 + fuel_rate) * 100) / 100

            # 更新数据
            sheet_data.at[index, "计费重量（美制）"] = jifei_weight
            sheet_data.at[index, "分区"] = partition
            sheet_data.at[index, "燃油"] = f"{fuel_rate * 100:.2f}%"
            sheet_data.at[index, "总金额"] = all_money

            # 格式化日期
            sheet_data.at[index, "第一枪\n扫描时间时间"] = (
                order_date.strftime("%Y-%m-%d") if pd.notna(order_date) else None
            )

            # 处理其他日期字段
            for date_field in ["美国出库\n时间", "送达时间"]:
                if date_field in sheet_data.columns:
                    # 使用pd.to_datetime转换日期，允许无法解析的值
                    date_value = pd.to_datetime(row[date_field], errors="coerce")
                    # 格式化日期，如果无法解析则设为None
                    sheet_data.at[index, date_field] = (
                        date_value.strftime("%Y-%m-%d")
                        if pd.notna(date_value)
                        else None
                    )

            # 计算是否偏远，根据快递单号 列来判断是fedex还是ups(1z开头)
            if str(row["快递单号"]).startswith("1Z"):
                for property_name, codes in ups_zip_data.items():
                    if row["邮编"] in codes:
                        sheet_data.at[index, "是否偏远"] = property_name
                        break
            else:
                for property_name, codes in fedex_zip_codes_by_category.items():
                    if row["邮编"] in codes:
                        sheet_data.at[index, "是否偏远"] = property_name
                        break

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            sheet_data.to_excel(writer, sheet_name="结果", index=False)

        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"output-{timestamp}.xlsx"

        # 保存到本地
        output_dir = os.path.join(os.getcwd(), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(output.getvalue())

        print(f"文件已保存到: {output_path}")

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@express_delivery_router.get("/get_ups_excel_template", summary="获取UPS报价模板Excel")
def get_ups_excel_template():
    excel_path = next(
        (
            os.path.join(os.getcwd(), "excel_template", f)
            for f in os.listdir(os.path.join(os.getcwd(), "excel_template"))
            if f.startswith("LAX发出-HTT")
        ),
        None,
    )
    if not excel_path:
        raise HTTPException(
            status_code=404, detail="未找到LAX发出-HTT开头的Excel模板文件"
        )
    return FileResponse(excel_path)


@express_delivery_router.post("/fedex_remoteaddresscheck", summary="联邦快递偏远地址校验Excel处理")
async def remoteaddresscheck(file: UploadFile = File(...)):
    """
    上传Excel文件，根据PDF中的邮政编码信息进行处理，并返回处理后的Excel文件。

    Args:
        file (UploadFile): 上传的Excel文件。

    Returns:
        StreamingResponse: 处理后的Excel文件流。

    Raises:
        HTTPException: 如果处理文件时出错。
    """
    try:
        # 检查上传的文件是否为Excel文件
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="请上传Excel文件")

        # 读取上传的Excel文件
        contents = await file.read()
        excel_file = io.BytesIO(contents)
        df = pd.read_excel(excel_file)

        # 定义PDF文件路径
        pdf_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
        )  # 确保PDF文件名正确

        # 检查PDF文件是否存在
        if not os.path.exists(pdf_path):
            raise HTTPException(
                status_code=404, detail="未找到Delivery Area Surcharge.pdf文件"
            )
        excel_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
        )
        # 使用process_excel_with_zip_codes函数处理Excel数据
        result_df = fedex_process_excel_with_zip_codes(excel_file, pdf_path,excel_path=excel_path)

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="结果", index=False)
        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"processed-{timestamp}.xlsx"

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@express_delivery_router.get("/get_fedex_remoteaddresscheck_effective_date", summary="获取联邦快递偏远地址PDF生效日期")
def get_fedex_remoteaddresscheck_effective_date():
    pdf_path = os.path.join(
        os.getcwd(),
        "file",
        "remoteaddresscheck",
        "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
    )
    effective_date = None
    try:
        with open(pdf_path, "rb") as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            page = pdf_reader.pages[0]
            text = page.extract_text()
            # 在这里添加提取日期的逻辑，例如使用正则表达式
            # 这里只是一个示例，你需要根据PDF的具体格式来提取
            # 示例：假设日期格式为 "Effective Date: YYYY-MM-DD"
            # 先尝试匹配Updated日期
            match = re.search(r"Updated\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})", text)
            if not match:
                # 如果没有Updated日期,则匹配Effective日期
                match = re.search(r"Effective\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})", text)
            if match:
                effective_date = match.group(1)
            else:
                effective_date = "日期未找到"
    except FileNotFoundError:
        effective_date = "文件未找到"
    except Exception as e:
        effective_date = f"读取文件出错: {str(e)}"

    return {"effective_date": effective_date}


@express_delivery_router.post("/ups_remoteaddresscheck", summary="UPS偏远地址校验Excel处理")
async def ups_remoteaddresscheck(file: UploadFile = File(...)):
    """
    上传Excel文件，根据PDF中的邮政编码信息进行处理，并返回处理后的Excel文件。
    """
    try:
        # 保存上传的Excel文件
        excel_file = io.BytesIO(await file.read())

        # 获取property定义Excel文件路径
        property_excel_path = os.path.join(
            os.getcwd(), "file", "remoteaddresscheck", "area-surcharge-zips-us-en.xlsx"
        )

        # 读取输入Excel
        input_df = pd.read_excel(excel_file)

        # 读取property定义Excel中的所有sheet
        xl = pd.ExcelFile(property_excel_path)

        # 存储code和property的映射关系
        code_property_map = {}

        # 遍历每个sheet获取code和property的对应关系
        for sheet_name in xl.sheet_names:
            df = pd.read_excel(property_excel_path, sheet_name=sheet_name)
            data = []
            # 遍历每一列
            for col in df.columns:
                for cell in df[col].dropna():
                    if isinstance(cell, str):
                        # 使用正则表达式提取数字
                        codes = re.findall(r"\b\d+\b", cell)
                        for code in codes:
                            if code == "00000":
                                continue
                            data.append(code)

                code_property_map[sheet_name] = data

        # 添加property列
        def get_property(code):
            # 检查邮编长度
            if len(str(code)) != 5:
                return "邮编错误，不足五位"

            for property_name, codes in code_property_map.items():
                if str(code) in codes:
                    return property_name
            return "Unknown"

        input_df["property"] = input_df["code"].apply(get_property)

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            input_df.to_excel(writer, index=False)
        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"zip_codes_processed_{timestamp}.xlsx"

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@express_delivery_router.get("/get_ups_remoteaddresscheck_effective_date", summary="获取UPS偏远地址Excel生效日期")
def get_ups_remoteaddresscheck_effective_date():
    pdf_path = os.path.join(
        os.getcwd(), "file", "remoteaddresscheck", "area-surcharge-zips-us-en.xlsx"
    )
    # 读取active sheet的B8单元格
    wb = pd.ExcelFile(pdf_path)
    active_sheet = wb.sheet_names[0]  # 获取第一个sheet作为active sheet
    df = pd.read_excel(pdf_path, sheet_name=active_sheet)
    return {"effective_date": df.iloc[6, 1].replace("Effective", "").strip()}


@express_delivery_router.post("/all_remoteaddresscheck_process", summary="批量校验Fedex/UPS偏远地址")
async def all_remoteaddresscheck_process(zip_code_str: str = Form(...)):
    # pdf_path = os.path.join(
    #     os.getcwd(),
    #     "file",
    #     "remoteaddresscheck",
    #     "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
    # )  # 确保PDF文件名正确
    excel_path = os.path.join(
        os.getcwd(),
        "file",
        "remoteaddresscheck",
        "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
    )
    # 检查PDF文件是否存在
    # if not os.path.exists(pdf_path):
    #     raise HTTPException(
    #         status_code=404, detail="未找到Delivery Area Surcharge.pdf文件"
    #     )  # 调用ups_process_excel_with_zip_codes函数
    if not os.path.exists(excel_path):
        raise HTTPException(
            status_code=404, detail="未找到DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.xlsx文件"
        )
    fedex_result = fedex_process_excel_with_zip_codes(zip_code_str,excel_path=excel_path)
    # fedex_result = extract_zip_codes_from_excel(zip_code_str)
    ups_result = ups_process_excel_with_zip_codes(zip_code_str)
    # 合并两个结果列表并按zip_code排序
    combined_result = sorted(fedex_result + ups_result, key=lambda x: x["zip_code"])
    usa_state_chinese = pd.read_excel(
        os.path.join(os.getcwd(), "file", "remoteaddresscheck", "美国州名.xlsx")
    )

    # 定义 property 中文映射
    property_chinese_mapping = {
        # "FEDEX": {
        #     "Contiguous U.S.": "普通偏远",
        #     "Contiguous U.S.: Extended": "超偏远",
        #     "Contiguous U.S.: Remote": "超级偏远",
        #     "Alaska": "阿拉斯加偏远",
        #     "Hawaii": "夏威夷偏远",
        #     "Intra-Hawaii": "夏威夷内部偏远",
        # },
         "FEDEX": {
            "DAS_ContUS": "普通偏远",
            "DAS_ContUSExt": "超偏远",
            "DAS_ContUSRem": "超级偏远",
            "DAS_Alaska": "阿拉斯加偏远",
            "DAS_Hawaii": "夏威夷偏远",
            "DAS_IntraHawaii": "夏威夷内部偏远",
        },
        "UPS": {
            "US 48 Zip": "普通偏远",
            "US 48 Zip DAS Extended": "超偏远",
            "Remote HI Zip": "夏威夷偏远",
            "Remote AK Zip": "阿拉斯加偏远",
            "Remote US 48 Zip": "超级偏远",
        },
    }

    # 遍历结果添加USPS信息和中文 property
    for item in combined_result:
        if item["property"] != "邮编错误,不足五位" and item["property"] != "Unknown":
            # usps_info = query_usps_zip(item['zip_code'])
            usps_info = None
            if usps_info and usps_info.get("resultStatus") == "SUCCESS":
                item["city"] = usps_info.get("defaultCity", "")
                item["state"] = usps_info.get("defaultState", "")
                if item["state"] in usa_state_chinese["美国州名缩写"].values:
                    # 找到对应的 列 ‘中文译名'
                    item["state"] += (
                        f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'
                    )

                # 获取避免使用的城市名称列表
                avoid_cities = [x["city"] for x in usps_info.get("nonAcceptList", [])]
                item["avoid_city"] = avoid_cities

            # 添加中文 property
            carrier_type = item["type"].upper()  # 获取承运商类型 (FEDEX 或 UPS)
            english_property = item["property"]  # 获取英文 property

            if (
                carrier_type in property_chinese_mapping
                and english_property in property_chinese_mapping[carrier_type]
            ):
                item["property_chinese"] = property_chinese_mapping[carrier_type][
                    english_property
                ]
            else:
                item["property_chinese"] = "未知偏远"  # 默认值
    return combined_result

