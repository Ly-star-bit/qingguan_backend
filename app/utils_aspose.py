import os
import sys
from datetime import datetime, timedelta
import time
from pathlib import Path
import random
import zipfile
import platform
# Add the AsposeCells_net directory to the path
aspose_dir = os.path.join(os.getcwd(), "AsposeCells_net")
sys.path.append(aspose_dir)
dll_version = "net40" if platform.system() == "Windows" else "net8.0"
print(os.path.join(aspose_dir, dll_version))
if dll_version == 'net8.0':
    os.environ["PYTHONNET_RUNTIME"] = "coreclr"

    # 自动查找 .NET 8.0 的 runtimeconfig.json
sys.path.append(os.path.join(aspose_dir, dll_version))
import clr

# Load Aspose.Cells.dll
clr.AddReference("Aspose.Cells")

# Import Aspose.Cells namespaces
import Aspose.Cells as Ac
from Aspose.Cells import Workbook, License, PdfSaveOptions, TextAlignmentType, SaveFormat
from Aspose.Cells.Rendering import PdfCompliance

# 设置许可证
license_path = os.path.join(aspose_dir, "Aspose.Total.NET.lic")
license = License()
license.SetLicense(license_path)

print("Aspose.Cells .NET 许可证设置成功！")
class AsposeCellsNETHandler:
    """
    Aspose.Cells for .NET 处理器类，用于Excel处理和转换
    """
    
    def __init__(self, dll_version="net40"):
        """
        初始化AsposeCellsNETHandler
        
        Args:
            dll_version (str): DLL版本，默认为"net40"
        """
        self.aspose_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "AsposeCells_net")
        self.dll_path = os.path.join(self.aspose_dir, dll_version)
        self.license_path = os.path.join(self.aspose_dir, "Aspose.Total.NET.lic")
        
        # 添加路径到系统路径
        sys.path.append(self.aspose_dir)
        sys.path.append(self.dll_path)
        
        # 设置许可证
        # self._set_license()
    
    def _set_license(self):
        """
        设置Aspose.Cells的许可证（内部方法）
        """
        try:
            # 创建License对象
            license = License()
            
            # 设置许可证
            license.SetLicense(self.license_path)
            print("Aspose.Cells .NET 许可证设置成功！")
            return True
        except Exception as e:
            print(f"设置Aspose.Cells .NET 许可证时出错: {str(e)}")
            return False
    
    def set_cell_value(self, sheet, row, column, value):
        """
        设置单元格值并应用格式
        
        Args:
            sheet: 工作表对象
            row (int): 行号（1-based）
            column (int): 列号（1-based）
            value: 要设置的值
        """
        cell = sheet.Cells[row - 1, column - 1]  # Adjust for zero-based index
        
        if isinstance(value, str) and value.startswith('='):
            # 如果 value 是一个公式
            cell.Formula = value
        else:
            # 否则，设置为普通值
            cell.PutValue(value)
        
        # 应用样式
        # style = cell.GetStyle()
        # style.IsTextWrapped = True
        # style.HorizontalAlignment = TextAlignmentType.Center
        # style.VerticalAlignment = TextAlignmentType.Center
        # cell.SetStyle(style)
    
    def excel_to_pdf(self, excel_path, pdf_save_path=None):
        """
        将Excel文件转换为PDF
        
        Args:
            excel_path (str): Excel文件路径
            pdf_save_path (str): PDF保存路径，如果为None则使用与Excel文件相同的路径
            
        Returns:
            str: 生成的PDF文件路径
        """
        try:
            # 打开Excel文件
            wb = Workbook(excel_path)
            
            # 删除名称为 "Evaluation Warning" 的工作表（如果存在）
            sheets = wb.Worksheets
            try:
                eval_warning_sheet = sheets["Evaluation Warning"]
                if eval_warning_sheet is not None:
                    sheets.RemoveAt("Evaluation Warning")
            except:
                pass  # 工作表不存在，忽略
            
            # 配置PDF保存选项
            saveOption = PdfSaveOptions()
            
            # 确保每个工作表单独保存为一个PDF页面
            saveOption.OnePagePerSheet = True
            
            # 计算公式
            saveOption.CalculateFormula = True
            
            # 设置字体相关选项
            saveOption.CheckWorkbookDefaultFont = True
            saveOption.CheckFontCompatibility = True
            saveOption.DefaultFont = "Arial"
            
            # 设置图像处理
            saveOption.ImageResample = (220, 85)
            
            # 设置其他相关选项
            saveOption.EmbedStandardWindowsFonts = True
            saveOption.ClearData = False
            saveOption.Compliance = PdfCompliance.Pdf14
            saveOption.DisplayDocTitle = True
            
            # 如果没有指定保存路径，则使用与 Excel 文件相同的路径
            if pdf_save_path is None:
                pdf_save_path = os.path.dirname(excel_path)
            
            # 获取Excel文件的文件名（不含扩展名）
            excel_name = os.path.splitext(os.path.basename(excel_path))[0]
            
            # 设置PDF文件的完整保存路径
            pdf_file = os.path.join(pdf_save_path, f"{excel_name}.pdf")
            
            # 保存为PDF
            wb.Save(pdf_file, saveOption)
            
            return pdf_file
        except Exception as e:
            raise Exception(f"Excel转换为PDF时出错: {str(e)}")
    
    def generate_excel_from_template(self, template_path, data, output_dir="file"):
        """
        根据模板生成Excel文件
        
        Args:
            template_path (str): 模板文件路径
            data (list): 数据列表
            output_dir (str): 输出目录
            
        Returns:
            str: 生成的Excel文件路径
        """
        try:
            # 打开模板文件
            wb = Workbook(template_path)
            civ_sheet = wb.Worksheets["CIV"]
            pl_sheet = wb.Worksheets["PL"]
            huomian_explaination_sheet = wb.Worksheets["豁免说明"] 
            
            # 填充CIV工作表内容
            if data and len(data) > 0:
                # 填充头部信息
                self.set_cell_value(civ_sheet, 1, 1, data[0].get("shipper_name", ""))
                self.set_cell_value(civ_sheet, 2, 1, data[0].get("shipper_address", ""))
                
                receiver_info = f"{data[0].get('receiver_name', '')}\n{data[0].get('receiver_address', '')}"
                self.set_cell_value(civ_sheet, 6, 1, receiver_info)
                
                # 生成文档编号
                today_minus_5 = datetime.now() - timedelta(days=5)
                formatted_date = today_minus_5.strftime("%Y%m%d")
                random_number = random.randint(1000, 9999)
                doc_number = f"{formatted_date}{random_number}"
                
                self.set_cell_value(civ_sheet, 6, 9, doc_number)
                self.set_cell_value(civ_sheet, 7, 9, doc_number)
                self.set_cell_value(civ_sheet, 8, 9, datetime.now().strftime("%Y/%m/%d"))
                
                # 填充PL工作表内容
                self.set_cell_value(pl_sheet, 1, 1, data[0].get("shipper_name", ""))
                self.set_cell_value(pl_sheet, 2, 1, data[0].get("shipper_address", ""))
                self.set_cell_value(pl_sheet, 5, 1, receiver_info)
                self.set_cell_value(pl_sheet, 5, 9, doc_number)
                self.set_cell_value(pl_sheet, 6, 9, doc_number)
                self.set_cell_value(pl_sheet, 7, 9, datetime.now().strftime("%Y/%m/%d"))
                
                # 填充数据行
                start_row = 14
                for index, item in enumerate(data):
                    civ_row = start_row + index
                    pl_row = start_row - 1 + index  # PL表从13行开始
                    huomian_row = 5 + index
                    # CIV表数据
                    self.set_cell_value(civ_sheet, civ_row, 1, index + 1)
                    self.set_cell_value(civ_sheet, civ_row, 2, item.get("HS_CODE", ""))
                    self.set_cell_value(civ_sheet, civ_row, 3, item.get("DESCRIPTION", ""))
                    self.set_cell_value(civ_sheet, civ_row, 4, item.get("quanity", ""))
                    self.set_cell_value(civ_sheet, civ_row, 5, item.get("danwei", ""))
                    self.set_cell_value(civ_sheet, civ_row, 6, item.get("unit_price", ""))
                    self.set_cell_value(civ_sheet, civ_row, 7, round(item.get("total_price", 0)))
                    self.set_cell_value(civ_sheet, civ_row, 8, item.get("texture", ""))
                    self.set_cell_value(civ_sheet, civ_row, 9, item.get("address_name", ""))
                    self.set_cell_value(civ_sheet, civ_row, 10, item.get("address", ""))
                    self.set_cell_value(civ_sheet, civ_row, 11, item.get("note", ""))
                    
                    # 自动调整行高
                    civ_sheet.AutoFitRow(civ_row - 1)
                    if index < len(data) - 1:
                        civ_sheet.Cells.InsertRow(civ_row)
                    
                    # PL表数据
                    self.set_cell_value(pl_sheet, pl_row, 1, index + 1)
                    self.set_cell_value(pl_sheet, pl_row, 2, item.get("HS_CODE", ""))
                    self.set_cell_value(pl_sheet, pl_row, 3, item.get("DESCRIPTION", ""))
                    self.set_cell_value(pl_sheet, pl_row, 4, item.get("quanity", ""))
                    self.set_cell_value(pl_sheet, pl_row, 5, item.get("danwei", ""))
                    self.set_cell_value(pl_sheet, pl_row, 6, item.get("carton", ""))
                    self.set_cell_value(pl_sheet, pl_row, 8, item.get("net_weight", ""))
                    self.set_cell_value(pl_sheet, pl_row, 9, item.get("GrossWeight", ""))
                    self.set_cell_value(pl_sheet, pl_row, 10, item.get("Volume", ""))
                    
                    # 自动调整行高
                    pl_sheet.AutoFitRow(pl_row - 1)
                    if index < len(data) - 1:
                        pl_sheet.Cells.InsertRow(pl_row)


                    # Fill 豁免说明
                    self.set_cell_value(huomian_explaination_sheet, huomian_row, 1, item["HS_CODE"])
                    self.set_cell_value(huomian_explaination_sheet, huomian_row, 2, item["DESCRIPTION"])
                    self.set_cell_value(huomian_explaination_sheet, huomian_row, 3, item["usage"])
                    self.set_cell_value(huomian_explaination_sheet, huomian_row, 4, item["note"])
                    self.set_cell_value(huomian_explaination_sheet, huomian_row, 5, item["note_explaination"])
                    # if item["huomian_file_name"]:
                    #     pic_path = os.path.join("./file/huomian_file/",item["huomian_file_name"])

                        # all_pic_path.append({"pic_path":pic_path,"new_name":item['DESCRIPTION']})
                    huomian_explaination_sheet.AutoFitRow(huomian_row - 1)
                    huomian_explaination_sheet.Cells.InsertRows(huomian_row)
            
            # 创建输出目录
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # 保存Excel文件
            master_bill_no = data[0].get("MasterBillNo", "unknown") if data else "unknown"
            output_path = os.path.join(output_dir, f"{time.time()}-{master_bill_no} CI&PL.xlsx")
            wb.CalculateFormula()
            wb.Save(output_path, SaveFormat.Xlsx)
            
            return output_path
        except Exception as e:
            raise Exception(f"生成Excel文件时出错: {str(e)}")

    def generate_fencangdan_file(self, data):
        """
        生成分舱单文件
        
        Args:
            data: 数据
            
        Returns:
            str: 生成的ZIP文件路径，包含所有PDF文件
        """
        try:
            fendan_path = r".\file\excel_template\分单模板 - 执行.xlsx"
            cangdan_path = r".\file\excel_template\舱单模板 - 执行.xlsx"

            cangdan_wb = Workbook(cangdan_path)
            cangdan_sheet = cangdan_wb.Worksheets["Sheet1"]
            
            # 处理舱单
            self.set_cell_value(cangdan_sheet, 3, 9, data['orderNumber'])
            self.set_cell_value(cangdan_sheet, 5, 9, f"{data['flight_no']}/{data.get('etd','')}")

            cangdan_row = 11
            for suborder in data['subOrders']:
                self.set_cell_value(cangdan_sheet, cangdan_row, 1, suborder['subOrderNumber'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 3, suborder['boxCount'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 4, suborder['grossWeight'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 5, data['startland'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 6, data['destination'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 7, suborder['sender'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 8, suborder['receiver'])
                self.set_cell_value(cangdan_sheet, cangdan_row, 9, suborder['natureOfName'])

                cangdan_sheet.AutoFitRow(cangdan_row - 1)
                cangdan_row += 1

            output_dir = Path("file/fencangdan/cangdan")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = str(output_dir / f"{time.time()}-{data['orderNumber']} .xlsx")

            cangdan_wb.Save(output_path, SaveFormat.Xlsx)
            print(f"Excel file generated: {output_path}")
            # 生成 PDF 文件
            cangdan_pdf_path = self.excel_to_pdf(output_path, 'pdf')
            
            #处理分单
            fendan_output_dir = Path("file/fencangdan/fendan")
            fendan_output_dir.mkdir(parents=True, exist_ok=True)
            fendan_wb = Workbook(fendan_path)
            fendan_sheet = fendan_wb.Worksheets["Sheet1"]
            all_pdf_paths = [cangdan_pdf_path]  # 收集所有PDF路径
            import pandas as pd
            #读取港口对应的城市
            df = pd.read_excel(r".\file\excel_template\港口信息.xlsx",sheet_name='Sheet2')

            # Select only the required columns
            df_selected = df[['港口英文名', '看这里']]

            # Convert to dictionary with '港口英文名' as keys and '看这里' as values
            city_data = df_selected.set_index('港口英文名')['看这里'].to_dict()
            startland_city = city_data.get(data['startland'],"")
            destination_city =  city_data.get(data['destination'],"")
            for suborder in data['subOrders']:
                self.set_cell_value(fendan_sheet, 1, 2, data['orderNumber'])
                self.set_cell_value(fendan_sheet, 1, 35, suborder['subOrderNumber'])
                self.set_cell_value(fendan_sheet, 4, 2, suborder['sender'])
                self.set_cell_value(fendan_sheet, 8, 2, suborder['receiver'])
                self.set_cell_value(fendan_sheet, 15, 2,startland_city )
                self.set_cell_value(fendan_sheet, 18, 2, data['destination'])
                #  data['startland']
                self.set_cell_value(fendan_sheet, 18, 3, data.get("shipcompany"))

                self.set_cell_value(fendan_sheet, 20, 2, destination_city)
                self.set_cell_value(fendan_sheet, 20, 9, data['flight_no'])
                self.set_cell_value(fendan_sheet, 28, 2, suborder['boxCount'])
                self.set_cell_value(fendan_sheet, 28, 3, suborder['grossWeight'])
                self.set_cell_value(fendan_sheet, 28, 9, suborder['grossWeight'])
                self.set_cell_value(fendan_sheet, 28, 34, suborder['natureOfName'] + f"\n\nVOL(CBM) :{suborder['volume']}")
                self.set_cell_value(fendan_sheet, 42, 14, datetime.now().strftime("%Y.%m.%d"))
                self.set_cell_value(fendan_sheet, 42, 33, startland_city)
                
                fendan_output_path = str(fendan_output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{data['orderNumber']}-{suborder['subOrderNumber']} .xlsx")
                fendan_wb.Save(fendan_output_path, SaveFormat.Xlsx)
                fendan_pdf_path = self.excel_to_pdf(fendan_output_path, 'pdf')
                all_pdf_paths.append(fendan_pdf_path)

            # 创建ZIP文件包含所有PDF
            zip_output_dir = Path("file/fencangdan/zip")
            zip_output_dir.mkdir(parents=True, exist_ok=True)
            zip_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{data['orderNumber']}.zip"
            zip_path = str(zip_output_dir / zip_filename)
            
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for pdf_path in all_pdf_paths:
                    # 将文件添加到ZIP中，只使用文件名而不包含路径
                    zipf.write(pdf_path, os.path.basename(pdf_path))
            
            return zip_path
        except Exception as e:
            raise Exception(f"生成分舱单文件时出错: {str(e)}")
    
    def generate_excel_from_template_canada(self, data, totalyugutax, currentcy_type="CAD"):
        """
        生成加拿大模板的Excel文件
        
        Args:
            data: 数据
            totalyugutax: 预估总税金
            currentcy_type: 货币类型
            
        Returns:
            str: 生成的PDF文件路径
        """
        try:
            template_path = "./file/excel_template/加拿大-清关发票箱单开发模板-0410.xlsx"
            wb = Workbook(template_path)
            civ_sheet = wb.Worksheets["CIV"]
            pl_sheet = wb.Worksheets["PL"]

            # 生成文档编号
            today_minus_5 = datetime.now() - timedelta(days=5)
            formatted_date = today_minus_5.strftime("%Y%m%d")
            random_number = random.randint(1000, 9999)
            result_1 = f"{formatted_date}{random_number}"
            
            self.set_cell_value(civ_sheet, 5, 8, result_1)
            self.set_cell_value(civ_sheet, 6, 8, result_1)
            self.set_cell_value(civ_sheet, 7, 8, datetime.now().strftime("%Y/%m/%d"))
            self.set_cell_value(civ_sheet, 9, 8, currentcy_type)

            self.set_cell_value(pl_sheet, 5, 8, result_1)
            self.set_cell_value(pl_sheet, 6, 8, result_1)
            self.set_cell_value(pl_sheet, 7, 8, datetime.now().strftime("%Y/%m/%d"))

            all_pic_path = []
            for index, item in enumerate(data):
                civ_row = 13 + index
                pl_row = 13 + index

                # Fill CIV
                self.set_cell_value(civ_sheet, civ_row, 1, f"=ROW()-ROW($A$13)+1")
                self.set_cell_value(civ_sheet, civ_row, 2, item["HS_CODE"])
                self.set_cell_value(civ_sheet, civ_row, 3, item["DESCRIPTION"])
                self.set_cell_value(civ_sheet, civ_row, 4, item["quanity"])
                self.set_cell_value(civ_sheet, civ_row, 5, item["danwei"])
                self.set_cell_value(civ_sheet, civ_row, 6, item["unit_price"])
                self.set_cell_value(civ_sheet, civ_row, 7, round(item["total_price"]))
                self.set_cell_value(civ_sheet, civ_row, 8, item["texture"])
                self.set_cell_value(civ_sheet, civ_row, 9, item["note"])

                civ_sheet.AutoFitRow(civ_row - 1)

                # Fill PL
                self.set_cell_value(pl_sheet, pl_row, 1, f"=ROW()-ROW($A$13)+1")
                self.set_cell_value(pl_sheet, pl_row, 2, item["HS_CODE"])
                self.set_cell_value(pl_sheet, pl_row, 3, item["DESCRIPTION"])
                self.set_cell_value(pl_sheet, pl_row, 4, item["quanity"])
                self.set_cell_value(pl_sheet, pl_row, 5, item["danwei"])
                self.set_cell_value(pl_sheet, pl_row, 6, item["carton"])
                self.set_cell_value(pl_sheet, pl_row, 8, item["net_weight"])
                self.set_cell_value(pl_sheet, pl_row, 9, item["GrossWeight"])
                self.set_cell_value(pl_sheet, pl_row, 10, item["Volume"])
                pl_sheet.AutoFitRow(pl_row - 1)

                if item["huomian_file_name"]:
                    pic_path = os.path.join("./file/huomian_file/", item["huomian_file_name"])
                    all_pic_path.append({"pic_path": pic_path, "new_name": item['DESCRIPTION']})

                if index == len(data) - 1:
                    # 如果是最后一个循环的数据，则不需要再添加一行了
                    break
                    
                # 在每个循环结束时增加一行，以避免覆盖
                if civ_sheet.Cells[civ_row + 1, 2].Value == "TOTAL":
                    civ_sheet.Cells.InsertRows(civ_row, 1)
                if pl_sheet.Cells[pl_row + 1, 2].Value == "TOTAL":
                    pl_sheet.Cells.InsertRows(pl_row, 1)

                civ_sheet.Cells.HideColumn(5)

            # Save the Excel file
            output_path = f"file/{time.time()}-{data[0]['MasterBillNo']} CI&PL-{totalyugutax}.xlsx"
            wb.CalculateFormula()
            wb.Save(output_path, SaveFormat.Xlsx)
            print(f"Excel file generated: {output_path}")

            # 生成 PDF 文件
            pdf_path = self.excel_to_pdf(output_path, 'pdf')
            return pdf_path
        except Exception as e:
            raise Exception(f"生成加拿大模板Excel文件时出错: {str(e)}")
    
    def shenzhen_customes_pdf_generate(self, data, filter_data):
        """
        生成深圳海关PDF文件
        
        Args:
            data: 数据
            filter_data: 过滤数据
            
        Returns:
            str: 生成的PDF文件路径
        """
        try:
            template_path = "HAWB模板-空+海_测试新版.xls"
            wb = Workbook(template_path)
            shenzhn_sheet = wb.Worksheets["S#-SZ-customs"]

            def set_cell_value_internal(sheet, row, column, value):
                if value is None:
                    value = ""
                cell = sheet.Cells[row - 1, column - 1]  # Adjust for zero-based index

                if isinstance(value, str) and value.startswith("="):
                    # 如果 value 是一个公式
                    cell.Formula = value
                else:
                    # 否则，设置为普通值
                    cell.PutValue(value)

            set_cell_value_internal(shenzhn_sheet, 5, 4, data["shipper_name"])
            set_cell_value_internal(shenzhn_sheet, 5, 17, data["master_bill_no"])

            set_cell_value_internal(shenzhn_sheet, 9, 4, data["receiver_name"])
            set_cell_value_internal(shenzhn_sheet, 16, 4, data["receiver_name"])
            set_cell_value_internal(shenzhn_sheet, 26, 10, data["total_boxes"])
            set_cell_value_internal(shenzhn_sheet, 26, 15, data["all_english_name"])
            set_cell_value_internal(shenzhn_sheet, 26, 22, str(data["gross_weight"]))
            set_cell_value_internal(shenzhn_sheet, 26, 25, str(data["volume"]))

            set_cell_value_internal(shenzhn_sheet, 48, 14, data["gross_weight"])
            set_cell_value_internal(shenzhn_sheet, 48, 19, str(data["volume"]) + "cbm")
            
            if filter_data["hblno"]:
                set_cell_value_internal(shenzhn_sheet, 5, 24, filter_data["hblno"])
            else:
                set_cell_value_internal(shenzhn_sheet, 5, 24, "系统未录入")

            set_cell_value_internal(shenzhn_sheet, 19, 11, filter_data["startland"])
            set_cell_value_internal(shenzhn_sheet, 21, 11, filter_data["startland"])
            set_cell_value_internal(shenzhn_sheet, 23, 4, filter_data["destination"])
            set_cell_value_internal(shenzhn_sheet, 23, 11, filter_data["destination"])
            
            # 航次
            set_cell_value_internal(shenzhn_sheet, 21, 4, filter_data["flight"])

            # 柜号
            set_cell_value_internal(shenzhn_sheet, 48, 5, filter_data["cabinetNo"])
            # 封条号
            set_cell_value_internal(shenzhn_sheet, 48, 6, filter_data["sealno"])
            # 柜型
            set_cell_value_internal(shenzhn_sheet, 48, 8, filter_data["cabinettype"])
            
            if filter_data["ATD"]:
                try:
                    atd_datetime = datetime.strptime(filter_data["ATD"], "%Y-%m-%d %H:%M")
                except ValueError:
                    # 如果解析失败，尝试解析不包含秒的格式
                    atd_datetime = datetime.strptime(filter_data["ATD"], "%Y-%m-%d %H:%M:%S")

                # 保留到年月日
                filter_data["ATD"] = atd_datetime.strftime("%Y-%m-%d")
                set_cell_value_internal(shenzhn_sheet, 50, 13, filter_data["ATD"])
            else:
                set_cell_value_internal(shenzhn_sheet, 50, 13, "系统未录入")

            ids = []
            for i in wb.Worksheets:
                origin_sheetname = i.Name
                if origin_sheetname == "S#-SZ-customs":
                    ids.append(i.Index)
            
            # 注意：SheetSet的使用方式可能需要调整，这里简化处理
            # new_SheetSet = SheetSet(ids)

            # 配置PDF保存选项
            saveOption = PdfSaveOptions()
            # saveOption.SheetSet = new_SheetSet
            # 确保每个工作表单独保存为一个PDF页面
            saveOption.OnePagePerSheet = True

            # 计算公式
            saveOption.CalculateFormula = True

            # 设置字体相关选项
            saveOption.CheckWorkbookDefaultFont = True
            saveOption.CheckFontCompatibility = True
            saveOption.DefaultFont = "Arial"

            # 设置图像处理
            saveOption.ImageResample = (220, 85)

            # 设置其他相关选项
            saveOption.EmbedStandardWindowsFonts = True
            saveOption.ClearData = False
            saveOption.Compliance = 0
            saveOption.DisplayDocTitle = True

            # 设置PDF文件的完整保存路径
            totalyugutax = data["other_data"]["totalyugutax"]
            pdf_file = f"./pdf/customs/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-{data['master_bill_no']}-{totalyugutax}.pdf"

            # 保存为PDF
            wb.Save(pdf_file, saveOption)
            return pdf_file
        except Exception as e:
            raise Exception(f"生成深圳海关PDF文件时出错: {str(e)}")

    def generate_admin_shenhe_template(self, data, totalyugutax):
        """
        生成ADMIN审核文件模板
        
        Args:
            data: 数据列表
            totalyugutax: 预估总税金
            
        Returns:
            str: 生成的Excel文件路径
        """
        try:
            start_time = time.time()
            template_path = "./file/excel_template/ADMIN-审核文件模板-0411.xlsx"
            
            # 打开模板文件
            wb = Workbook(template_path)
            sheet = wb.Worksheets[0]  # 获取第一个工作表（活动工作表）

            # 生成文档编号
            today_minus_5 = datetime.now() - timedelta(days=5)
            formatted_date = today_minus_5.strftime("%Y%m%d")
            random_number = random.randint(1000, 9999)
            result_1 = f"{formatted_date}{random_number}"

            # 填充头部信息
            self.set_cell_value(sheet, 1, 1, data[0]["shipper_name"])
            self.set_cell_value(sheet, 2, 1, data[0]["shipper_address"])
            
            receiver_info = f"{data[0]['receiver_name']}\n{data[0]['receiver_address']}"
            self.set_cell_value(sheet, 6, 1, receiver_info)
            
            self.set_cell_value(sheet, 6, 14, result_1)
            self.set_cell_value(sheet, 7, 14, result_1)
            self.set_cell_value(sheet, 8, 14, datetime.now().strftime("%Y/%m/%d"))
            self.set_cell_value(sheet, 10, 14, data[0]["MasterBillNo"])

            # 填充数据行
            start_row = 14
            for index, item in enumerate(data):
                civ_row = start_row + index
                
                # 设置值
                self.set_cell_value(sheet, civ_row, 1, index + 1)
                self.set_cell_value(sheet, civ_row, 2, item["HS_CODE"])
                self.set_cell_value(sheet, civ_row, 3, item["duty"])
                self.set_cell_value(sheet, civ_row, 4, item["additional_duty"])
                self.set_cell_value(sheet, civ_row, 5, item["DESCRIPTION"])
                self.set_cell_value(sheet, civ_row, 6, item["ChineseName"])
                self.set_cell_value(sheet, civ_row, 7, f"=I{civ_row}*J{civ_row}")
                self.set_cell_value(sheet, civ_row, 8, item["danwei"])
                self.set_cell_value(sheet, civ_row, 9, int(item["quanity"] / item["carton"]) if item["carton"] != 0 else 0)
                self.set_cell_value(sheet, civ_row, 10, item["carton"])
                self.set_cell_value(sheet, civ_row, 11, item["unit_price"])
                self.set_cell_value(sheet, civ_row, 12, f"=K{civ_row}*G{civ_row}")
                self.set_cell_value(sheet, civ_row, 13, item["texture"])
                self.set_cell_value(sheet, civ_row, 14, item["note"])
                self.set_cell_value(sheet, civ_row, 15, f"=round(P{civ_row}*0.8,2)")
                self.set_cell_value(sheet, civ_row, 16, item["GrossWeight"])
                self.set_cell_value(sheet, civ_row, 17, item["Volume"])

                # 处理additional_duty为百分比或小数的情况
                def convert_duty_value(duty_str):
                    if isinstance(duty_str, str) and duty_str.endswith('%'):
                        try:
                            return float(duty_str.strip('%')) / 100
                        except Exception:
                            return 0
                    else:
                        try:
                            return float(duty_str)
                        except Exception:
                            return 0

                additional_duty_value = convert_duty_value(item["additional_duty"])
                duty_value = convert_duty_value(item["duty"])
                
                self.set_cell_value(sheet, civ_row, 18, f"=round(D{civ_row}*L{civ_row},2)")
                self.set_cell_value(sheet, civ_row, 19, f"=round(C{civ_row}*L{civ_row},2)")

                # 自动调整行高
                sheet.AutoFitRow(civ_row - 1)
                
                # 插入新行以避免覆盖
                if index < len(data) - 1:  # 不为最后一项插入新行
                    sheet.Cells.InsertRows(civ_row, 1)

            # 计算最后一行的位置
            last_data_row = start_row + len(data) - 1
            
            # 设置求和公式，从14行开始到当前行
            self.set_cell_value(sheet, last_data_row + 3, 7, f"=SUM(G14:G{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 10, f"=SUM(J14:J{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 12, f"=SUM(L14:L{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 15, f"=SUM(O14:O{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 16, f"=SUM(P14:P{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 17, f"=SUM(Q14:Q{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 18, f"=SUM(R14:R{last_data_row})")
            self.set_cell_value(sheet, last_data_row + 3, 19, f"=SUM(S14:S{last_data_row})")

            # 计算平均单箱重量
            gross_weights = [item.get("GrossWeight", 0) for item in data if not item.get("single_weight")]
            cartons = [item.get("carton", 1) for item in data if not item.get("single_weight") and item.get("carton", 1) != 0]
            
            if cartons and sum(cartons) != 0:
                average_single_weight = sum(gross_weights) / sum(cartons)
            else:
                average_single_weight = 0

            # 添加单箱重量
            self.set_cell_value(sheet, last_data_row + 5, 15, "单箱重量: ")
            self.set_cell_value(sheet, last_data_row + 5, 16, f"{round(average_single_weight, 2)}")

            # 添加其他信息
            self.set_cell_value(sheet, last_data_row + 6, 15, "预估总税金: ")
            self.set_cell_value(sheet, last_data_row + 6, 16, f"=round({data[0].get('estimated_tax_amount', 0)}, 2)")
            
            self.set_cell_value(sheet, last_data_row + 7, 15, "货值比: ")
            self.set_cell_value(sheet, last_data_row + 7, 16, f"=round(L{last_data_row + 3}/P{last_data_row + 3}, 2)")

            self.set_cell_value(sheet, last_data_row + 8, 15, "美国税率: ")
            self.set_cell_value(sheet, last_data_row + 8, 16, f"{data[0].get('rate', '')}")

            self.set_cell_value(sheet, last_data_row + 9, 15, "税金单价: ")
            self.set_cell_value(sheet, last_data_row + 9, 16, f"{data[0].get('estimated_tax_rate_cny_per_kg', '')}")

            # 保存文件
            output_path = f"file/{time.time()}-{data[0]['MasterBillNo']} CI&PL-{totalyugutax}_admin_审核.xlsx"
            wb.CalculateFormula()
            wb.Save(output_path, SaveFormat.Xlsx)
            
            end_time = time.time()
            print(f"generate_admin_shenhe_template 审核模板 运行时间: {end_time - start_time:.2f} 秒")
            
            return output_path
        except Exception as e:
            raise Exception(f"生成ADMIN审核文件模板时出错: {str(e)}")

    def generate_admin_shenhe_canada_template(self, data, totalyugutax):
        """
        生成加拿大ADMIN审核文件模板
        
        Args:
            data: 数据列表
            totalyugutax: 预估总税金
            
        Returns:
            str: 生成的Excel文件路径
        """
        try:
            template_path = "./file/excel_template/加拿大_admin_审核-模板-0606.xlsx"
            
            # 打开模板文件
            wb = Workbook(template_path)
            sheet = wb.Worksheets[0]  # 获取第一个工作表（活动工作表）

            # 生成文档编号
            today_minus_5 = datetime.now() - timedelta(days=5)
            formatted_date = today_minus_5.strftime("%Y%m%d")
            random_number = random.randint(1000, 9999)
            result_1 = f"{formatted_date}{random_number}"

            # 填充头部信息
            self.set_cell_value(sheet, 5, 14, result_1)
            self.set_cell_value(sheet, 6, 14, result_1)
            self.set_cell_value(sheet, 7, 14, datetime.now().strftime("%Y/%m/%d"))
            self.set_cell_value(sheet, 9, 14, data[0]["MasterBillNo"])

            # 填充数据行
            start_row = 13
            for index, item in enumerate(data):
                civ_row = start_row + index
                
                # 设置值
                self.set_cell_value(sheet, civ_row, 1, index + 1)
                self.set_cell_value(sheet, civ_row, 2, item["HS_CODE"])
                self.set_cell_value(sheet, civ_row, 3, item["duty"])
                self.set_cell_value(sheet, civ_row, 4, item["additional_duty"])
                self.set_cell_value(sheet, civ_row, 5, item["DESCRIPTION"])
                self.set_cell_value(sheet, civ_row, 6, item["ChineseName"])
                self.set_cell_value(sheet, civ_row, 7, item["quanity"])
                self.set_cell_value(sheet, civ_row, 8, item["danwei"])
                self.set_cell_value(sheet, civ_row, 9, int(item["quanity"] / item["carton"]) if item["carton"] != 0 else 0)
                self.set_cell_value(sheet, civ_row, 10, item["carton"])
                self.set_cell_value(sheet, civ_row, 11, item["unit_price"])
                self.set_cell_value(sheet, civ_row, 12, item["total_price"])
                self.set_cell_value(sheet, civ_row, 13, item["texture"])
                self.set_cell_value(sheet, civ_row, 14, item["note"])
                self.set_cell_value(sheet, civ_row, 15, item["net_weight"])
                self.set_cell_value(sheet, civ_row, 16, item["GrossWeight"])
                self.set_cell_value(sheet, civ_row, 17, item["Volume"])

                # 处理additional_duty为百分比或小数的情况
                def convert_duty_value(duty_str):
                    if isinstance(duty_str, str) and duty_str.endswith('%'):
                        try:
                            return float(duty_str.strip('%')) / 100
                        except Exception:
                            return 0
                    else:
                        try:
                            return float(duty_str)
                        except Exception:
                            return 0

                additional_duty_value = convert_duty_value(item["additional_duty"])
                duty_value = convert_duty_value(item["duty"])
                
                self.set_cell_value(sheet, civ_row, 18, round(float(item["total_price"])) * additional_duty_value)
                self.set_cell_value(sheet, civ_row, 19, round(float(item["total_price"])) * duty_value)

                # 自动调整行高
                sheet.AutoFitRow(civ_row - 1)
                
                # 插入新行以避免覆盖
                if index < len(data) - 1:  # 不为最后一项插入新行
                    sheet.Cells.InsertRows(civ_row, 1)

            # 计算最后一行的位置
            last_data_row = start_row + len(data) - 1

            # 计算平均单箱重量
            gross_weights = [item.get("GrossWeight", 0) for item in data if not item.get("single_weight")]
            cartons = [item.get("carton", 1) for item in data if not item.get("single_weight") and item.get("carton", 1) != 0]
            
            if cartons and sum(cartons) != 0:
                average_single_weight = sum(gross_weights) / sum(cartons)
            else:
                average_single_weight = 0

            # 添加单箱重量
            self.set_cell_value(sheet, last_data_row + 7, 15, "单箱重量: ")
            self.set_cell_value(sheet, last_data_row + 7, 16, f"{round(average_single_weight, 2)}")

            # 添加其他信息
            self.set_cell_value(sheet, last_data_row + 8, 15, "预估总税金: ")
            self.set_cell_value(sheet, last_data_row + 8, 16, f"{data[0].get('estimated_tax_amount', '')}")

            # 保存文件
            output_path = f"file/{time.time()}-{data[0]['MasterBillNo']} CI&PL-{totalyugutax}_admin_审核.xlsx"
            wb.CalculateFormula()
            wb.Save(output_path, SaveFormat.Xlsx)
            
            return output_path
        except Exception as e:
            raise Exception(f"生成加拿大ADMIN审核文件模板时出错: {str(e)}")

handler = AsposeCellsNETHandler()
# 兼容性函数，保持与原有utils.py类似的接口
import time

def excel2pdf(excel_path: str, pdf_save_path: str = None) -> str:
    """
    将Excel文件转换为PDF（兼容性函数）
    
    Args:
        excel_path (str): Excel文件路径
        pdf_save_path (str): PDF保存路径
        
    Returns:
        str: 生成的PDF文件路径
    """
    start_time = time.time()
    # handler = AsposeCellsNETHandler()
    result = handler.excel_to_pdf(excel_path, pdf_save_path)
    end_time = time.time()
    print(f"excel2pdf 运行时间: {end_time - start_time:.2f} 秒")
    return result


def generate_excel_from_template_test(data, totalyugutax, port, template_path="./file/excel_template/副本清关发票箱单模板 - 0918更新.xlsx"):
    """
    根据模板生成Excel文件并转换为PDF（兼容性函数）
    
    Args:
        data: 数据
        totalyugutax: 预估总税金
        port: 港口
        template_path: 模板路径
        
    Returns:
        str: 生成的PDF文件路径
    """
    start_time = time.time()
    # handler = AsposeCellsNETHandler()
    
    # 生成Excel文件
    excel_path = handler.generate_excel_from_template(template_path, data)
    
    # 转换为PDF
    pdf_path = handler.excel_to_pdf(excel_path, 'pdf')
    end_time = time.time()
    print(f"generate_excel_from_template_test 运行时间: {end_time - start_time:.2f} 秒")
    
    return pdf_path


def generate_fencangdan_file(data):
    """
    生成分舱单文件
    
    Args:
        data: 数据
        
    Returns:
        str: 生成的ZIP文件路径，包含所有PDF文件
    """
    start_time = time.time()
    # handler = AsposeCellsNETHandler()
    result = handler.generate_fencangdan_file(data)
    end_time = time.time()
    print(f"generate_fencangdan_file 运行时间: {end_time - start_time:.2f} 秒")
    return result


def generate_excel_from_template_canada(data, totalyugutax, currentcy_type="CAD"):
    """
    生成加拿大模板的Excel文件
    
    Args:
        data: 数据
        totalyugutax: 预估总税金
        currentcy_type: 货币类型
        
    Returns:
        str: 生成的PDF文件路径
    """
    start_time = time.time()
    # handler = AsposeCellsNETHandler()
    result = handler.generate_excel_from_template_canada(data, totalyugutax, currentcy_type)
    end_time = time.time()
    print(f"generate_excel_from_template_canada 运行时间: {end_time - start_time:.2f} 秒")
    return result


def shenzhen_customes_pdf_gennerate(data, filter_data):
    """
    生成深圳海关PDF文件
    
    Args:
        data: 数据
        filter_data: 过滤数据
        
    Returns:
        str: 生成的PDF文件路径
    """
    start_time = time.time()
    # handler = AsposeCellsNETHandler()
    result = handler.shenzhen_customes_pdf_generate(data, filter_data)
    end_time = time.time()
    print(f"shenzhen_customes_pdf_gennerate 运行时间: {end_time - start_time:.2f} 秒")
    return result


def generate_admin_shenhe_template(data, totalyugutax):
    """
    生成ADMIN审核文件模板（兼容性函数）
    
    Args:
        data: 数据列表
        totalyugutax: 预估总税金
        
    Returns:
        str: 生成的Excel文件路径
    """
    start_time = time.time()
    result = handler.generate_admin_shenhe_template(data, totalyugutax)
    end_time = time.time()
    print(f"generate_admin_shenhe_template 审核模板 运行时间: {end_time - start_time:.2f} 秒")
    return result


def generate_admin_shenhe_canada_template(data, totalyugutax):
    """
    生成加拿大ADMIN审核文件模板（兼容性函数）
    
    Args:
        data: 数据列表
        totalyugutax: 预估总税金
        
    Returns:
        str: 生成的Excel文件路径
    """
    start_time = time.time()
    result = handler.generate_admin_shenhe_canada_template(data, totalyugutax)
    end_time = time.time()
    print(f"generate_admin_shenhe_canada_template 审核模板 运行时间: {end_time - start_time:.2f} 秒")
    return result


# def generate_excel_from_template_test(data, totalyugutax, port, template_path="./file/excel_template/副本清关发票箱单模板 - 0918更新.xlsx"):
#     """
#     根据模板生成Excel文件并转换为PDF（兼容性函数）
    
#     Args:
#         data: 数据
#         totalyugutax: 预估总税金
#         port: 港口
#         template_path: 模板路径
        
#     Returns:
#         str: 生成的PDF文件路径
#     """
#     # handler = AsposeCellsNETHandler()
    
#     # 生成Excel文件
#     excel_path = handler.generate_excel_from_template(template_path, data)
    
#     # 转换为PDF
#     pdf_path = handler.excel_to_pdf(excel_path, 'pdf')
    
#     return pdf_path


# def generate_fencangdan_file(data):
#     """
#     生成分舱单文件
    
#     Args:
#         data: 数据
        
#     Returns:
#         str: 生成的ZIP文件路径，包含所有PDF文件
#     """
#     # handler = AsposeCellsNETHandler()
#     return handler.generate_fencangdan_file(data)


# def generate_excel_from_template_canada(data, totalyugutax, currentcy_type="CAD"):
#     """
#     生成加拿大模板的Excel文件
    
#     Args:
#         data: 数据
#         totalyugutax: 预估总税金
#         currentcy_type: 货币类型
        
#     Returns:
#         str: 生成的PDF文件路径
#     """
#     # handler = AsposeCellsNETHandler()
#     return handler.generate_excel_from_template_canada(data, totalyugutax, currentcy_type)


# def shenzhen_customes_pdf_gennerate(data, filter_data):
#     """
#     生成深圳海关PDF文件
    
#     Args:
#         data: 数据
#         filter_data: 过滤数据
        
#     Returns:
#         str: 生成的PDF文件路径
#     """
#     # handler = AsposeCellsNETHandler()
#     return handler.shenzhen_customes_pdf_generate(data, filter_data)