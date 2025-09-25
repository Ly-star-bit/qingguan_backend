import os
import sys
import clr

class AsposeCellsHandler:
    """
    Aspose.Cells处理器类，用于加载DLL、设置许可证、读取Excel文件并进行转换或保存操作
    """
    
    def __init__(self, dll_version="net40"):
        """
        初始化AsposeCellsHandler
        
        Args:
            dll_version (str): DLL版本，默认为"net8.0"
        """
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.dll_path = os.path.join(self.current_dir, dll_version)
        self.license_path = os.path.join(self.current_dir, "Aspose.Total.NET.lic")
        self.workbook = None
        self.file_path = None
        
        # 添加路径到系统路径
        sys.path.append(self.current_dir)
        sys.path.append(self.dll_path)
        
        # 加载Aspose.Cells.dll
        clr.AddReference("Aspose.Cells")
        
        # 导入Aspose.Cells命名空间
        import Aspose.Cells as Ac
        
        # 保存引用以便在方法中使用
        self.Ac = Ac
        
        # 设置许可证
        self._set_license()
    
    def _set_license(self):
        """
        设置Aspose.Cells的许可证（内部方法）
        """
        try:
            # 创建License对象
            license = self.Ac.License()
            
            # 设置许可证
            license.SetLicense(self.license_path)
            
            print("许可证设置成功！")
            print(f"许可证文件路径: {self.license_path}")
            return True
        except Exception as e:
            print(f"设置许可证时出错: {str(e)}")
            return False
    
    def create_workbook(self):
        """
        创建一个新的工作簿
        
        Returns:
            Workbook: 创建的工作簿对象
        """
        try:
            self.workbook = self.Ac.Workbook()
            print("工作簿创建成功！")
            return self.workbook
        except Exception as e:
            print(f"创建工作簿时出错: {str(e)}")
            return None
    
    def load_file(self, file_path):
        """
        读取Excel文件
        
        Args:
            file_path (str): Excel文件路径
            
        Returns:
            bool: 读取是否成功
        """
        try:
            self.file_path = file_path
            self.workbook = self.Ac.Workbook(file_path)
            print(f"Excel文件读取成功！文件路径: {file_path}")
            return True
        except Exception as e:
            print(f"读取Excel文件时出错: {str(e)}")
            return False
    
    def save_as_excel(self, output_path=None):
        """
        将工作簿保存为Excel文件
        
        Args:
            output_path (str): 输出文件路径，如果为None则使用默认路径
            
        Returns:
            bool: 保存是否成功
        """
        if not self.workbook:
            print("错误：没有可保存的工作簿，请先读取Excel文件！")
            return False
        
        try:
            # 如果没有指定输出路径，使用默认路径
            if not output_path:
                if self.file_path:
                    base_name = os.path.splitext(os.path.basename(self.file_path))[0]
                    output_path = os.path.join(self.current_dir, f"{base_name}_saved.xlsx")
                else:
                    output_path = os.path.join(self.current_dir, "output.xlsx")
            
            # 保存为Excel文件
            self.workbook.Save(output_path)
            
            print(f"Excel文件保存成功！保存路径: {output_path}")
            return True
        except Exception as e:
            print(f"保存Excel文件时出错: {str(e)}")
            return False
    
    def save_as_pdf(self, output_path=None):
        """
        将工作簿保存为PDF文件
        
        Args:
            output_path (str): 输出文件路径，如果为None则使用默认路径
            
        Returns:
            bool: 保存是否成功
        """
        if not self.workbook:
            print("错误：没有可保存的工作簿，请先读取Excel文件！")
            return False
        
        try:
            # 如果没有指定输出路径，使用默认路径
            if not output_path:
                if self.file_path:
                    base_name = os.path.splitext(os.path.basename(self.file_path))[0]
                    output_path = os.path.join(self.current_dir, f"{base_name}.pdf")
                else:
                    output_path = os.path.join(self.current_dir, "output.pdf")
            
            # 保存为PDF文件
            self.workbook.Save(output_path, self.Ac.SaveFormat.Pdf)
            
            print(f"PDF文件保存成功！保存路径: {output_path}")
            return True
        except Exception as e:
            print(f"保存PDF文件时出错: {str(e)}")
            return False
    
    def convert_to_pdf(self, pdf_path=None):
        """
        将已读取的Excel文件转换为PDF
        
        Args:
            pdf_path (str): 输出PDF文件路径，如果为None则使用默认路径
            
        Returns:
            bool: 转换是否成功
        """
        if not self.workbook:
            print("错误：没有可转换的工作簿，请先读取Excel文件！")
            return False
        
        try:
            # 如果没有指定PDF路径，使用默认路径
            if not pdf_path:
                if self.file_path:
                    base_name = os.path.splitext(os.path.basename(self.file_path))[0]
                    pdf_path = os.path.join(self.current_dir, f"{base_name}.pdf")
                else:
                    pdf_path = os.path.join(self.current_dir, "output.pdf")
            
            # 转换为PDF
            self.workbook.Save(pdf_path, self.Ac.SaveFormat.Pdf)
            
            print(f"Excel转换为PDF成功！PDF保存路径: {pdf_path}")
            return True
        except Exception as e:
            print(f"转换Excel到PDF时出错: {str(e)}")
            return False
    
    def write_to_cell(self, sheet_name_or_index, cell_name, value):
        """
        向指定单元格写入数据
        
        Args:
            sheet_name_or_index (str or int): 工作表名称或索引
            cell_name (str): 单元格名称，如"A1"
            value: 要写入的值
            
        Returns:
            bool: 写入是否成功
        """
        if not self.workbook:
            print("错误：没有可操作的工作簿！")
            return False
        
        try:
            # 获取工作表
            if isinstance(sheet_name_or_index, str):
                worksheet = self.workbook.Worksheets[sheet_name_or_index]
            else:
                worksheet = self.workbook.Worksheets[sheet_name_or_index]
            
            # 写入数据
            worksheet.Cells[cell_name].PutValue(value)
            print(f"数据写入成功！工作表: {sheet_name_or_index}, 单元格: {cell_name}, 值: {value}")
            return True
        except Exception as e:
            print(f"写入单元格数据时出错: {str(e)}")
            return False
    
    def test_functionality(self):
        """
        测试Aspose.Cells功能，特别关注试用模式下的限制
        """
        try:
            # 创建一个新的工作簿
            self.create_workbook()
            
            # 测试1: 基本功能测试
            print("\n=== 测试1: 基本功能测试 ===")
            self.write_to_cell(0, "A1", "Hello from Aspose.Cells!")
            
            # 测试2: 数据行数限制测试
            print("\n=== 测试2: 数据行数限制测试 ===")
            worksheet = self.workbook.Worksheets[0]
            
            # 写入超过100行数据（测试试用模式的行数限制）
            for i in range(1, 150):  # 写入149行数据
                worksheet.Cells[f"A{i}"].PutValue(f"测试数据行 {i}")
                worksheet.Cells[f"B{i}"].PutValue(f"值 {i}")
            
            print("已写入149行测试数据")
            
            # 测试3: 多工作表测试
            print("\n=== 测试3: 多工作表测试 ===")
            for sheet_idx in range(1, 5):  # 创建4个额外的工作表
                # 添加新工作表并获取其索引
                sheet_index = self.workbook.Worksheets.Add()
                # 通过索引获取工作表对象
                new_sheet = self.workbook.Worksheets[sheet_index]
                new_sheet.Name = f"测试工作表 {sheet_idx}"
                new_sheet.Cells["A1"].PutValue(f"这是工作表 {sheet_idx}")
                print(f"创建工作表: {new_sheet.Name}")
            
            # 测试4: 保存为Excel并检查限制
            print("\n=== 测试4: 保存为Excel并检查限制 ===")
            excel_output_path = os.path.join(self.current_dir, "test_output.xlsx")
            self.save_as_excel(excel_output_path)
            
            # 检查文件是否存在
            if os.path.exists(excel_output_path):
                file_size = os.path.getsize(excel_output_path)
                print(f"Excel文件已保存，大小: {file_size} 字节")
                
                # 读取保存的文件，检查数据是否完整
                test_workbook = self.Ac.Workbook(excel_output_path)
                saved_worksheet = test_workbook.Worksheets[0]
                
                # 检查行数
                last_row = saved_worksheet.Cells.Rows.Count
                print(f"保存的工作表总行数: {last_row}")
                
                # 检查特定行是否存在数据
                test_row_50 = saved_worksheet.Cells["A50"].StringValue
                test_row_100 = saved_worksheet.Cells["A100"].StringValue
                test_row_150 = saved_worksheet.Cells["A150"].StringValue
                
                print(f"第50行数据: {test_row_50}")
                print(f"第100行数据: {test_row_100}")
                print(f"第150行数据: {test_row_150}")
                
                # 检查工作表数量
                sheets_count = test_workbook.Worksheets.Count
                print(f"保存的工作簿中工作表数量: {sheets_count}")
                
                # 检查是否有水印（通过查找特定文本）
                has_watermark = False
                for sheet_idx in range(sheets_count):
                    sheet = test_workbook.Worksheets[sheet_idx]
                    # 检查页眉页脚
                    try:
                        # 尝试获取页眉和页脚的不同方式
                        header = ""
                        footer = ""
                        
                        # 方法1: 尝试通过HeaderFooter属性访问
                        if hasattr(sheet.PageSetup, "HeaderFooter"):
                            header_footer = sheet.PageSetup.HeaderFooter
                            if hasattr(header_footer, "FirstHeader"):
                                header += header_footer.FirstHeader
                            if hasattr(header_footer, "FirstFooter"):
                                footer += header_footer.FirstFooter
                            if hasattr(header_footer, "OddHeader"):
                                header += header_footer.OddHeader
                            if hasattr(header_footer, "OddFooter"):
                                footer += header_footer.OddFooter
                        
                        # 方法2: 尝试直接访问属性
                        elif hasattr(sheet.PageSetup, "LeftHeader"):
                            header += sheet.PageSetup.LeftHeader
                            header += sheet.PageSetup.CenterHeader
                            header += sheet.PageSetup.RightHeader
                            footer += sheet.PageSetup.LeftFooter
                            footer += sheet.PageSetup.CenterFooter
                            footer += sheet.PageSetup.RightFooter
                        
                        # 检查页眉页脚中是否包含评价水印
                        if "evaluation" in header.lower() or "evaluation" in footer.lower():
                            has_watermark = True
                            print(f"在工作表 '{sheet.Name}' 的页眉/页脚中发现水印文本")
                            print(f"页眉内容: {header}")
                            print(f"页脚内容: {footer}")
                    except Exception as e:
                        print(f"检查工作表 '{sheet.Name}' 的页眉页脚时出错: {str(e)}")
                    
                    # 检查单元格内容
                    for row in range(1, 10):  # 检查前10行
                        for col in range(1, 5):  # 检查前4列
                            try:
                                # 使用正确的方式访问单元格
                                cell = sheet.Cells[row-1, col-1]  # Aspose.Cells使用0-based索引
                                cell_value = cell.StringValue
                                if "evaluation" in cell_value.lower() or "aspose" in cell_value.lower():
                                    has_watermark = True
                                    print(f"在工作表 '{sheet.Name}' 的单元格 {row},{col} 中发现水印文本: {cell_value}")
                            except Exception as e:
                                print(f"检查单元格 {row},{col} 时出错: {str(e)}")
                
                if has_watermark:
                    print("⚠️ 检测到评价水印 - 这是试用模式的典型限制")
                else:
                    print("✓ 未检测到明显的评价水印")
                
                test_workbook.Dispose()
            else:
                print("❌ Excel文件保存失败")
            
            # 测试5: 转换为PDF并检查限制
            print("\n=== 测试5: 转换为PDF并检查限制 ===")
            pdf_output_path = os.path.join(self.current_dir, "test_output.pdf")
            self.convert_to_pdf(pdf_output_path)
            
            if os.path.exists(pdf_output_path):
                pdf_size = os.path.getsize(pdf_output_path)
                print(f"PDF文件已保存，大小: {pdf_size} 字节")
            else:
                print("❌ PDF文件保存失败")
            
            print("\n=== 测试总结 ===")
            print("如果看到以下情况，说明Aspose.Cells运行在试用模式下：")
            print("1. 保存的文件中包含评价水印")
            print("2. 数据行数被限制（例如只保存前100行）")
            print("3. 工作表数量被限制")
            print("4. 某些高级功能可能不可用")
            
            return True
        except Exception as e:
            print(f"测试功能时出错: {str(e)}")
            return False

if __name__ == "__main__":
    # 创建AsposeCellsHandler实例
    handler = AsposeCellsHandler()
    
    # 测试功能
    handler.test_functionality()
    
    # 使用示例：
    # 1. 创建处理器实例
    # excel_handler = AsposeCellsHandler()
    
    # 2. 读取Excel文件
    # excel_handler.load_file("path/to/excel/file.xlsx")
    
    # 3. 保存为Excel文件
    # excel_handler.save_as_excel("path/to/output.xlsx")
    
    # 4. 转换为PDF
    # excel_handler.convert_to_pdf("path/to/output.pdf")