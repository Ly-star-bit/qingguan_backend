"""
AI驱动的Excel数据提取工具
使用直接 API 调用分析不同格式的Excel并提取数据
顺序处理多个Sheet
"""

import sys
import json
import re
from typing import List, Dict, Any, Optional
from pathlib import Path

# 设置 UTF-8 编码（解决 Windows 控制台编码问题）
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
import openai

from dotenv import load_dotenv
load_dotenv()

from .global_analyzer import GlobalAnalyzer
from .data_map_cache import DataMapCache
from .models import DataMap, ProductRecord


# ==================== Excel 提取器 ====================

class AutoGenExcelExtractor:
    """
    Excel提取器
    使用直接 API 调用分析Excel结构并提取数据
    顺序处理多个Sheet
    """

    SYSTEM_PROMPT = """你是一个专业的Excel数据分析专家。

你的职责：
1. 分析Excel文件的结构，识别sheet名称和数据格式
2. 根据用户需求（提取HSCODE、品名、数量、单价、材质等字段），找到对应的列
3. 使用pandas读取Excel数据，必要时进行数据清洗和转换
4. 从申报要素或规格型号中提取材质信息（成分含量）

关键技能：
- 熟练使用pandas读取Excel文件
- 熟练使用正则表达式提取特定内容
- 能够处理不同格式的中文Excel文件
- 理解报关单、发票、装箱单等外贸文档的数据结构

重要：最终输出只需5列：HSCODE、品名、件数、单价、材质

输出格式：
```json
[
  {"HSCODE": "1234567890", "品名": "商品A", "件数": "100", "单价": "10.5", "材质": "90%锦纶10%氨纶"},
  {"HSCODE": "0987654321", "品名": "商品B", "件数": "200", "单价": "20.0", "材质": "100%棉"}
]
```

请直接分析数据并输出JSON结果，不要输出其他内容。"""

    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1", model: str = "gpt-4o"):
        """
        初始化提取器

        Args:
            api_key: API密钥
            base_url: API基础URL
            model: 使用的模型
        """
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

        # 初始化 OpenAI 客户端
        self.client = openai.OpenAI(
            api_key=api_key,
            base_url=base_url
        )

    def _analyze_single_sheet(self, file_path: str, sheet_name: str, data_map: Optional[DataMap] = None) -> List[ProductRecord]:
        """
        分析单个Sheet并提取数据
        策略：两阶段方案
        - 阶段1（AI分析一次）：找到表头行、确定列位置、确认提取策略
        - 阶段2（直接提取）：按位置读取数据，整理成最终格式

        如果传入了 data_map（第一阶段 GlobalAnalyzer 生成），则直接使用其结构信息，
        不再重新分析结构。
        """
        try:
            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            total_rows = len(df_raw)
            total_cols = len(df_raw.columns)

            print(f"[调试] Sheet '{sheet_name}': 共 {total_rows} 行 x {total_cols} 列")

            # ========== 检查是否有预先生成的 data_map 结构 ==========
            structure = None

            if data_map and data_map.data_distribution:
                # 直接从 data_map 转换结构
                print(f"[调试] 使用第一阶段 data_map 的结构信息")
                structure = self._convert_datamap_to_structure(data_map, sheet_name)
                if structure:
                    print(f"[调试] data_map 结构转换成功: {structure}")
                else:
                    print(f"[调试] data_map 结构转换失败，将重新分析")

            # 如果没有有效的 data_map 结构，则调用 AI 重新分析
            if not structure:
                print(f"[调试] 开始调用 AI 分析 Sheet 结构...")
                structure = self._analyze_sheet_structure(file_path, sheet_name, df_raw)

            if not structure:
                print(f"[调试] Sheet '{sheet_name}' 无法分析结构，跳过")
                return []

            header_row = structure['header_row']  # 表头行索引
            field_cols = structure['field_cols']  # 各字段对应的列索引
            data_start_row = structure['data_start_row']  # 数据起始行

            print(f"[调试] 表头行: {header_row}")
            print(f"[调试] 字段列位置: {field_cols}")
            print(f"[调试] 数据起始行: {data_start_row}")

            # ========== 阶段2：按结构提取（不调AI，直接读）==========
            all_records = []
            for row_idx in range(data_start_row, total_rows):
                row_data = df_raw.iloc[row_idx]

                # 提取各字段值
                hscode = self._safe_get_value(row_data, field_cols.get('HSCODE'))
                product_name = self._safe_get_value(row_data, field_cols.get('品名'))
                quantity = self._safe_get_value(row_data, field_cols.get('件数'))
                unit_price = self._safe_get_value(row_data, field_cols.get('单价'))
                material = self._safe_get_value(row_data, field_cols.get('材质'))

                # 跳过空行或无效行
                if not hscode and not product_name:
                    continue

                # 如果HSCODE不是10位数字，跳过
                if hscode:
                    hscode_clean = str(hscode).replace('.', '').strip()
                    if not (hscode_clean.isdigit() and len(hscode_clean) >= 10):
                        continue

                all_records.append(ProductRecord(
                    hscode=hscode,
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    material=material
                ))

            print(f"[Sheet分析完成] {sheet_name}: 共提取 {len(all_records)} 条记录")
            return all_records

        except Exception as e:
            import traceback
            print(f"[Sheet分析错误] {sheet_name}: {e}")
            print(f"[Sheet分析错误] 详细 traceback: {traceback.format_exc()}")
            return []

    def _analyze_sheet_structure(self, file_path: str, sheet_name: str, df_raw: pd.DataFrame) -> Optional[Dict]:
        """
        阶段1：分析Sheet结构
        扫描前50行，找到表头行，确定各字段列索引

        返回结构：
        {
            'header_row': 3,       # 表头行索引
            'field_cols': {        # 各字段对应的列索引
                'HSCODE': 1,
                '品名': 2,
                '件数': 5,
                '单价': 6,
                '材质': 7
            },
            'data_start_row': 4    # 数据起始行
        }
        """
        total_rows = len(df_raw)
        scan_rows = min(50, total_rows)  # 最多扫描前50行

        # 收集前scan_rows行的数据发给AI分析
        sample_data = []
        for i in range(scan_rows):
            row_dict = {}
            for j in range(len(df_raw.columns)):
                val = df_raw.iloc[i, j]
                if pd.notna(val):
                    row_dict[str(j)] = str(val)
            sample_data.append(row_dict)

        # AI分析 prompt
        task_prompt = (
            f"Sheet名称: {sheet_name}\n"
            f"总行数: {total_rows}\n"
            f"扫描前{scan_rows}行找表头结构\n\n"
            f"数据格式（列名为数字索引0,1,2...）：\n"
            f"{json.dumps(sample_data[:20], ensure_ascii=False, indent=2)}\n\n"  # 先发前20行预览
            f"请分析这个Sheet的结构，找出：\n"
            f"1. 表头行是第几行（索引从0开始）？表头通常包含'HSCODE'、'品名'、'数量'、'件数'、'单价'、'材质'等列名\n"
            f"2. 各目标字段对应的列索引（数字索引）是几？\n"
            f"   - HSCODE：海关编码，10位数字\n"
            f"   - 品名：商品名称\n"
            f"   - 件数：数量/件数\n"
            f"   - 单价：单价\n"
            f"   - 材质：成分含量/材质（如需从'申报要素'列提取则标注）\n"
            f"3. 数据从第几行开始？\n\n"
            f"严格按以下JSON格式输出（不得附加任何文字）：\n"
            f'{{"header_row": 行索引, "field_cols": {{"HSCODE": 列索引, "品名": 列索引, "件数": 列索引, "单价": 列索引, "材质": 列索引}}, "data_start_row": 数据起始行索引, "material_needs_extract": true/false, "material_source_col": 列索引或null}}'
        )

        print(f"[调试] 开始分析Sheet结构...")

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是Excel结构分析工具，只输出JSON，不输出任何其他内容。"},
                {"role": "user", "content": task_prompt}
            ],
            temperature=0,
            max_tokens=2048
        )

        ai_response = response.choices[0].message.content
        print(f"[调试] 结构分析AI返回: {ai_response[:500]}")

        # 解析AI返回
        structure = self._parse_structure_response(ai_response)
        return structure

    def _parse_structure_response(self, ai_response: str) -> Optional[Dict]:
        """解析AI返回的结构分析结果"""
        # 尝试从后往前找JSON对象
        last_bracket_pos = ai_response.rfind('}')
        while last_bracket_pos > 0:
            start_pos = ai_response.rfind('{', 0, last_bracket_pos)
            if start_pos < 0:
                break
            candidate = ai_response[start_pos:last_bracket_pos + 1]
            try:
                data = json.loads(candidate)
                # 验证必要字段
                if isinstance(data, dict) and 'field_cols' in data:
                    # 确保所有必要字段都存在
                    required_fields = ['HSCODE', '品名', '件数', '单价', '材质']
                    field_cols = data.get('field_cols', {})
                    if all(f in field_cols for f in required_fields):
                        print(f"[调试] 结构解析成功: {data}")
                        return data
            except json.JSONDecodeError:
                pass
            last_bracket_pos = ai_response.rfind('}', 0, last_bracket_pos - 1)

        print(f"[调试] 结构解析失败")
        return None

    def _safe_get_value(self, row_data, col_idx: Optional[int]) -> str:
        """安全获取单元格值"""
        if col_idx is None:
            return ""
        try:
            val = row_data.iloc[col_idx] if hasattr(row_data, 'iloc') else row_data[col_idx]
            if pd.notna(val):
                return str(val).strip()
            return ""
        except:
            return ""

    def _find_declaration_col(self, sheet_df, header_row: int, data_start: int) -> Optional[int]:
        """
        扫描数据行，找到包含成分含量（%）的列

        Returns:
            列索引，如果没找到返回None
        """
        # 扫描数据行，找包含"%"的列
        search_start = max(0, data_start - 5)
        search_end = min(len(sheet_df), data_start + 20)

        col_declaration_count = {}  # {col_idx: count}

        for row_idx in range(search_start, search_end):
            row_data = sheet_df.iloc[row_idx]
            for col_idx in range(len(row_data)):
                val = self._safe_get_value(row_data, col_idx)
                # 检查是否包含成分含量特征（数字+%）
                if '%' in val and any(c.isdigit() for c in val):
                    if col_idx not in col_declaration_count:
                        col_declaration_count[col_idx] = 0
                    col_declaration_count[col_idx] += 1

        if col_declaration_count:
            # 返回出现次数最多的列
            return max(col_declaration_count.keys(), key=lambda k: col_declaration_count[k])

        return None

    def _convert_datamap_to_structure(self, data_map: DataMap, sheet_name: str) -> Optional[Dict]:
        """
        将 GlobalAnalyzer 生成的 DataMap 转换为内部结构格式

        优先使用 data_map.sheet_structures[sheet_name] 中的详细信息，
        如果没有则回退到 data_distribution 的基本信息。
        """
        try:
            # 优先使用 sheet_structures
            if data_map.sheet_structures and sheet_name in data_map.sheet_structures:
                sheet_struct = data_map.sheet_structures[sheet_name]
                print(f"[调试] 使用 sheet_structures 中的结构信息: {sheet_struct}")
                return {
                    "header_row": sheet_struct.get("header_row", 0),
                    "field_cols": sheet_struct.get("field_cols", {}),
                    "data_start_row": sheet_struct.get("data_start_row", 1),
                    "source": "datamap_sheet_structure"
                }

            # 回退到 data_distribution（只有列信息，没有行信息）
            field_cols = {}
            target_fields = ['HSCODE', '品名', '件数', '单价', '材质']

            for field in target_fields:
                if field in data_map.data_distribution:
                    field_info = data_map.data_distribution[field]
                    col_str = field_info.get("column", "")

                    # 将列名字母转换为索引（如 "B" -> 1, "C" -> 2）
                    if col_str and isinstance(col_str, str):
                        col_str = col_str.upper()
                        if len(col_str) == 1:
                            col_idx = ord(col_str) - ord('A')
                        elif len(col_str) == 2:
                            col_idx = (ord(col_str[0]) - ord('A') + 1) * 26 + (ord(col_str[1]) - ord('A'))
                        else:
                            col_idx = None
                    elif isinstance(col_str, int):
                        col_idx = col_str
                    else:
                        col_idx = None

                    if col_idx is not None:
                        field_cols[field] = col_idx

            if not field_cols or len(field_cols) < 2:
                print(f"[调试] DataMap 字段信息不完整: {field_cols}")
                return None

            print(f"[调试] sheet_structures 中没有找到 '{sheet_name}'，使用 data_distribution 的基本信息")
            # 默认表头行0，数据起始行1（不知道实际值）
            return {
                "header_row": 0,
                "field_cols": field_cols,
                "data_start_row": 1,
                "source": "datamap_distribution"
            }

        except Exception as e:
            print(f"[调试] DataMap 转换失败: {e}")
            return None

    def _parse_ai_response(self, final_response: str) -> List[ProductRecord]:
        """
        解析AI响应内容
        策略1：从后往前找合法JSON数组
        策略2：正则兜底，从推理文本中提取字段
        """
        records = []

        # 策略1：从后往前定位最后一个合法 JSON 数组
        last_bracket_pos = final_response.rfind(']')
        while last_bracket_pos > 0:
            start_pos = final_response.rfind('[', 0, last_bracket_pos)
            if start_pos < 0:
                break
            candidate = final_response[start_pos:last_bracket_pos + 1]
            try:
                data = json.loads(candidate)
                # 必须是非空的字典列表，排除空列表或字符串列表
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    print(f"[调试] JSON解析成功，共 {len(data)} 条")
                    for item in data:
                        records.append(ProductRecord(
                            hscode=str(item.get("HSCODE") or ""),
                            product_name=str(item.get("品名") or ""),
                            quantity=str(item.get("件数") or ""),
                            unit_price=str(item.get("单价") or ""),
                            material=str(item.get("材质") or "")
                        ))
                    return records
            except json.JSONDecodeError:
                last_bracket_pos = final_response.rfind(']', 0, last_bracket_pos - 1)

        # 策略2：正则兜底，针对 "HSCODE: xxx, 品名: xxx, ..." 推理文本格式
        print(f"[调试] JSON解析失败，启用正则兜底")
        fallback_matches = re.findall(
            r'HSCODE[：:]\s*(\d+)[,，\s]+品名[：:]\s*([^,，\n]+)[,，\s]+'
            r'件数[：:]\s*(\d+)[,，\s]+单价[：:]\s*([\d.]+)[,，\s]+材质[：:]\s*([^\n]*)',
            final_response
        )
        if fallback_matches:
            print(f"[调试] 正则兜底匹配到 {len(fallback_matches)} 条")
            for m in fallback_matches:
                material = m[4].strip()
                if material in ('空', '无', '未知', 'N/A', '-', ''):
                    material = ''
                records.append(ProductRecord(
                    hscode=m[0].strip(),
                    product_name=m[1].strip(),
                    quantity=m[2].strip(),
                    unit_price=m[3].strip(),
                    material=material
                ))
        else:
            print(f"[调试] 正则兜底也无匹配，AI响应格式不可解析")

        return records

    def extract(self, file_path: str, output_path: str = "extracted_data.xlsx") -> List[ProductRecord]:
        """
        提取Excel中的产品数据（顺序处理多个Sheet）

        Args:
            file_path: Excel文件路径
            output_path: 输出文件路径

        Returns:
            提取的产品记录列表
        """
        file_path_abs = str(Path(file_path).resolve())
        all_records = []

        print(f"[开始] 读取Excel文件: {file_path_abs}")

        # 先读取所有Sheet名称（不调用AI，快速获取结构）
        try:
            xl = pd.ExcelFile(file_path_abs)
            sheet_names = xl.sheet_names
            print(f"[信息] 发现 {len(sheet_names)} 个Sheet: {sheet_names}")
            xl.close()  # 立即关闭文件句柄
        except Exception as e:
            print(f"[错误] 无法读取Excel文件: {e}")
            return all_records

        # 读取所有Sheet数据
        sheets_data = {}
        for name in sheet_names:
            try:
                df = pd.read_excel(xl, sheet_name=name)
                sheets_data[name] = df
                print(f"[信息] Sheet '{name}': {len(df)} 行 x {len(df.columns)} 列")
            except Exception as e:
                print(f"[警告] 无法读取Sheet '{name}': {e}")

        # 顺序处理所有Sheet
        print(f"[开始] 顺序分析 {len(sheets_data)} 个Sheet")
        for name in sheets_data.keys():
            try:
                records = self._analyze_single_sheet(file_path_abs, name)
                all_records.extend(records)
                print(f"[完成] Sheet '{name}': +{len(records)} 条")
            except Exception as e:
                print(f"[错误] Sheet '{name}': {e}")

        print(f"[完成] 共提取 {len(all_records)} 条记录")

        # 保存结果到Excel
        if all_records:
            self._save_to_excel(all_records, output_path)

        return all_records

    def _save_to_excel(self, records: List[ProductRecord], output_path: str):
        """保存记录到Excel文件（只包含5列）"""
        try:
            df = pd.DataFrame([r.to_dict() for r in records])

            # 确保列顺序
            columns = ["HSCODE", "品名", "件数", "单价", "材质"]
            df = df[columns]

            df.to_excel(output_path, index=False, engine='openpyxl')
            print(f"[保存] 结果已保存到: {output_path}")
            print(f"[预览]\n{df.head()}")
        except Exception as e:
            print(f"[错误] 保存失败: {e}")

    def extract_smart(self, file_path: str, output_path: str = "extracted_data.xlsx",
                      use_cache: bool = True, confidence_threshold: float = 0.8) -> List[ProductRecord]:
        """
        两阶段智能提取 - 基于全局分析的数据提取

        阶段1: GlobalAnalyzer 分析 Excel 结构，生成数据地图
        阶段2: 根据数据地图选择最优提取策略执行提取

        Args:
            file_path: Excel文件路径
            output_path: 输出文件路径
            use_cache: 是否使用数据地图缓存
            confidence_threshold: 置信度阈值，高于此值自动执行，低于此值回退到原方法

        Returns:
            提取的产品记录列表
        """
        file_path_abs = str(Path(file_path).resolve())
        all_records = []

        print(f"[智能提取] 开始处理: {file_path_abs}")

        # 初始化缓存管理器
        cache = DataMapCache()

        # Step 1: 尝试从缓存获取数据地图
        data_map = None
        if use_cache:
            data_map = cache.get(file_path_abs)
            if data_map:
                print(f"[智能提取] 从缓存加载数据地图成功")

        # Step 2: 如果缓存无效，调用 GlobalAnalyzer 生成数据地图
        if not data_map:
            print(f"[智能提取] 正在调用全局分析器...")
            analyzer = GlobalAnalyzer(
                api_key=self.api_key,
                base_url=self.base_url,
                model=self.model
            )
            data_map = analyzer.analyze_excel_structure(file_path_abs)

            # 保存到缓存
            if data_map and use_cache:
                cache.set(file_path_abs, data_map)

        # Step 3: 根据数据地图选择提取策略
        if not data_map:
            print(f"[智能提取] 数据地图生成失败，回退到传统方法")
            return self.extract(file_path, output_path)

        strategy = data_map.extraction_strategy
        confidence = getattr(data_map, 'confidence', 1.0)

        print(f"[智能提取] 策略: {strategy}, 置信度: {confidence}")

        # 置信度检查：低于阈值回退到传统方法
        if confidence < confidence_threshold:
            print(f"[智能提取] 置信度 {confidence} < 阈值 {confidence_threshold}，回退到传统方法")
            return self.extract(file_path, output_path)

        # Step 4: 根据策略执行提取
        if strategy == "single_sheet":
            print(f"[智能提取] 使用单Sheet提取策略")
            all_records = self._extract_single_sheet_strategy(file_path_abs, data_map)
        elif strategy == "multi_sheet_join":
            print(f"[智能提取] 使用多Sheet关联提取策略")
            all_records = self._extract_multi_sheet_strategy(file_path_abs, data_map)
        else:
            print(f"[智能提取] 未知策略 '{strategy}'，回退到传统方法")
            return self.extract(file_path, output_path)

        print(f"[智能提取] 共提取 {len(all_records)} 条记录")

        # 保存结果到Excel
        if all_records:
            self._save_to_excel(all_records, output_path)

        return all_records

    def _extract_single_sheet_strategy(self, file_path: str, data_map: DataMap) -> List[ProductRecord]:
        """
        单Sheet提取策略 - 数据全部在一个Sheet中

        直接使用 data_map 中的结构信息提取，不调用AI。
        """
        all_records = []

        try:
            # 获取主Sheet名称（从 data_distribution 找）
            field_sheet_map = {}
            for field_name, field_info in data_map.data_distribution.items():
                sheet = field_info.get("sheet")
                if sheet:
                    field_sheet_map[field_name] = sheet

            main_sheet = list(set(field_sheet_map.values()))[0] if field_sheet_map else None
            if not main_sheet:
                print(f"[错误] 无法确定主Sheet")
                return all_records

            # 获取该Sheet的结构
            if not data_map.sheet_structures or main_sheet not in data_map.sheet_structures:
                print(f"[错误] 主Sheet '{main_sheet}' 没有结构信息")
                return all_records

            sheet_structure = data_map.sheet_structures[main_sheet]
            header_row = sheet_structure.get('header_row', 0)
            field_cols = sheet_structure.get('field_cols', {})
            data_start_row = sheet_structure.get('data_start_row', header_row + 1)

            print(f"[单Sheet提取] Sheet: {main_sheet}, 表头行: {header_row}, 数据起始: {data_start_row}")
            print(f"[单Sheet提取] 字段列: {field_cols}")

            # 读取Excel（保留原始索引）
            xl = pd.ExcelFile(file_path)
            df_raw = pd.read_excel(xl, sheet_name=main_sheet, header=None)
            xl.close()  # 立即关闭文件句柄
            total_rows = len(df_raw)

            # 修正 data_start_row：如果 AI 判断的 header_row 行的第一列是纯数字序号（1,2,3...），
            # 而 data_start_row 行也是数字序号，说明 AI 把数据行误判为表头了，应该往前一行
            if header_row < total_rows:
                header_row_data = df_raw.iloc[header_row]
                header_first_col = self._safe_get_value(header_row_data, 0)
                if header_first_col and str(header_first_col).strip().isdigit():
                    # header_row 第一列是数字，说明 AI 把数据行误判为表头了
                    if data_start_row > header_row + 1:
                        data_start_row = header_row + 1
                        print(f"[单Sheet提取] 修正 data_start_row: {header_row + 2} -> {data_start_row} (header_row被误判)")
                    elif data_start_row == header_row + 1:
                        # 这种情况需要再检查：header_row+1 的行第一列是不是也是数字
                        if header_row + 1 < total_rows:
                            next_row_data = df_raw.iloc[header_row + 1]
                            next_first_col = self._safe_get_value(next_row_data, 0)
                            if next_first_col and str(next_first_col).strip().isdigit():
                                # header_row 和 data_start_row 都是纯数字，header_row 确实是数据行
                                data_start_row = header_row
                                print(f"[单Sheet提取] 修正 data_start_row: {header_row + 1} -> {data_start_row} (header_row是数据行)")

            # 按结构提取数据
            for row_idx in range(data_start_row, total_rows):
                row_data = df_raw.iloc[row_idx]

                hscode = self._safe_get_value(row_data, field_cols.get('HSCODE'))
                product_name = self._safe_get_value(row_data, field_cols.get('品名'))
                quantity = self._safe_get_value(row_data, field_cols.get('件数'))
                unit_price = self._safe_get_value(row_data, field_cols.get('单价'))
                raw_material = self._safe_get_value(row_data, field_cols.get('材质'))
                material = self._extract_material_from_declaration(raw_material)

                # 跳过空行
                if not hscode and not product_name:
                    continue

                # HSCODE验证（如果是空的或无效的也保留，因为可能是发票类型文件）
                if hscode:
                    hscode_clean = str(hscode).replace('.', '').strip()
                    if not (hscode_clean.isdigit() and len(hscode_clean) >= 10):
                        # 不是有效HSCODE，可能是其他编号，保留但不作为HSCODE
                        pass

                all_records.append(ProductRecord(
                    hscode=hscode or '',
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    material=material
                ))

            print(f"[单Sheet提取] 共提取 {len(all_records)} 条记录")

        except Exception as e:
            import traceback
            print(f"[错误] 单Sheet提取失败: {e}")
            print(f"[错误] traceback: {traceback.format_exc()}")

        return all_records

    def _extract_multi_sheet_strategy(self, file_path: str, data_map: DataMap) -> List[ProductRecord]:
        """
        多Sheet关联提取策略 - 数据分散在多个Sheet

        策略：
        1. 找出每个字段所在的Sheet
        2. 以HSCODE+单价组合为关联键，从各Sheet提取数据（不去重）
        3. 合并到统一的记录中
        """
        import re
        all_records = []

        try:
            # 读取 Excel（用 header=None 保留原始索引）
            xl = pd.ExcelFile(file_path)
            sheets_data = {}  # {sheet_name: df_raw}

            for name in xl.sheet_names:
                try:
                    df = pd.read_excel(xl, sheet_name=name, header=None)
                    sheets_data[name] = df
                except Exception as e:
                    print(f"[警告] 无法读取Sheet '{name}': {e}")

            xl.close()  # 立即关闭文件句柄

            # 根据 data_distribution 构建字段到Sheet的映射
            field_sheet_map = {}  # {field_name: sheet_name}
            for field_name, field_info in data_map.data_distribution.items():
                sheet = field_info.get("sheet")
                if sheet:
                    field_sheet_map[field_name] = sheet

            # 收集所有涉及的 Sheet
            involved_sheets = set(field_sheet_map.values())
            print(f"[多Sheet关联] 涉及的Sheet: {involved_sheets}")
            print(f"[多Sheet关联] 字段分布: {field_sheet_map}")

            # 确定主Sheet（包含 HSCODE 的 Sheet）
            main_sheet = field_sheet_map.get("HSCODE") or field_sheet_map.get("品名")
            if not main_sheet:
                print(f"[错误] 多Sheet关联：没有找到主Sheet")
                return all_records

            print(f"[多Sheet关联] 主Sheet: {main_sheet}")

            # 获取主Sheet的结构信息
            main_structure = None
            if data_map.sheet_structures and main_sheet in data_map.sheet_structures:
                main_structure = data_map.sheet_structures[main_sheet]

            if not main_structure:
                print(f"[错误] 多Sheet关联：主Sheet '{main_sheet}' 没有结构信息")
                return all_records

            main_field_cols = main_structure.get('field_cols', {})
            main_data_start = main_structure.get('data_start_row', 1)

            # 从主Sheet提取所有数据行（不去重，用 (HSCODE, 单价) 作为组合键）
            main_df = sheets_data.get(main_sheet)
            if main_df is None:
                print(f"[错误] 多Sheet关联：找不到主Sheet '{main_sheet}'")
                return all_records

            # 用列表存储每一条记录
            main_records = []  # [{field: value, ...}, ...]

            for row_idx in range(main_data_start, len(main_df)):
                row_data = main_df.iloc[row_idx]
                record = {}

                # 提取主Sheet中的各字段
                for field_name, col_idx in main_field_cols.items():
                    val = self._safe_get_value(row_data, col_idx)
                    record[field_name] = val

                # 获取品名用于后续关联（品名必须要有）
                product_name = record.get('品名', '')
                if not product_name:
                    continue  # 没有品名的行跳过

                # 跳过表头行（品名是列名的情况）
                if product_name in ('品名', '商品名称', '商品名称及规格型号', '报关品名', 'Description', 'Commodity and Specifications'):
                    continue

                # 跳过HSCODE是列名的情况
                hscode = record.get('HSCODE', '')
                if hscode in ('HSCODE', '商品编号', '海关编码', '商编'):
                    continue

                main_records.append(record)

            print(f"[多Sheet关联] 主Sheet共 {len(main_records)} 条记录（不去重）")

            # 从所有其他Sheet补充字段
            # 使用更健壮的方式：遍历每个Sheet，根据其field_cols检查所有可提取字段
            for other_sheet in involved_sheets:
                if other_sheet == main_sheet:
                    continue  # 主Sheet已经处理过了

                print(f"\n[多Sheet关联] 处理 Sheet '{other_sheet}'")

                sheet_df = sheets_data.get(other_sheet)
                if sheet_df is None:
                    continue

                # 获取该Sheet的结构信息
                sheet_structure = None
                if data_map.sheet_structures and other_sheet in data_map.sheet_structures:
                    sheet_structure = data_map.sheet_structures[other_sheet]

                if not sheet_structure:
                    print(f"[警告] Sheet '{other_sheet}' 没有结构信息，跳过")
                    continue

                sheet_field_cols = sheet_structure.get('field_cols', {})
                sheet_data_start = sheet_structure.get('data_start_row', 1)

                print(f"[多Sheet关联] Sheet '{other_sheet}' 的字段: {sheet_field_cols}")

                # 找到该Sheet中的关联键列（HSCODE或品名）
                sheet_key_col = sheet_field_cols.get('HSCODE') if sheet_field_cols.get('HSCODE') is not None else sheet_field_cols.get('品名')
                if sheet_key_col is None:
                    print(f"[警告] Sheet '{other_sheet}' 没有关联键列（HSCODE或品名），跳过")
                    continue

                # 确定该Sheet能提取哪些目标字段
                # 不仅限于预定义的字段，而是检查field_cols中的所有字段
                # 排除关联键本身和主Sheet已提取的字段
                target_fields = set(['件数', '单价', '材质', 'HSCODE', '品名'])
                available_fields = []  # [(field_name, col_idx), ...]
                for field_name, col_idx in sheet_field_cols.items():
                    if field_name in target_fields and col_idx is not None:
                        # 排除关联键本身
                        if field_name in ('HSCODE', '品名'):
                            continue
                        available_fields.append((field_name, col_idx))

                if not available_fields:
                    print(f"[多Sheet关联] Sheet '{other_sheet}' 没有可提取的目标字段，跳过")
                    continue

                print(f"[多Sheet关联] Sheet '{other_sheet}' 可提取字段: {[f[0] for f in available_fields]}")

                # 遍历该Sheet，建立关联键 -> 各字段值 的映射
                # 注意：如果关联键是品名但值是英文，可能需要同时用多个列作为关联键
                key_field_values_map = {}  # {key_val: {field_name: value}}

                # 获取所有可能的关联键列
                possible_key_cols = []
                if sheet_field_cols.get('HSCODE') is not None:
                    possible_key_cols.append(('HSCODE', sheet_field_cols.get('HSCODE')))
                if sheet_field_cols.get('品名') is not None:
                    possible_key_cols.append(('品名', sheet_field_cols.get('品名')))

                for row_idx in range(sheet_data_start, len(sheet_df)):
                    row_data = sheet_df.iloc[row_idx]

                    # 尝试多个可能的关联键
                    key_vals = {}  # {key_type: key_val}
                    for key_type, col_idx in possible_key_cols:
                        val = self._safe_get_value(row_data, col_idx)
                        if val:
                            key_val = str(val).strip()
                            key_vals[key_type] = key_val

                            # 如果品名是英文（不包含中文字符），尝试在同一行找中文品名列
                            if key_type == '品名' and not any('\u4e00' <= c <= '\u9fff' for c in key_val):
                                # 扫描该行的其他列，找包含中文的列作为品名
                                for alt_col_idx in range(len(row_data)):
                                    if alt_col_idx == col_idx:
                                        continue
                                    alt_val = self._safe_get_value(row_data, alt_col_idx)
                                    if alt_val and any('\u4e00' <= c <= '\u9fff' for c in str(alt_val)):
                                        # 找到中文品名，同时用它建立映射
                                        alt_key_val = str(alt_val).strip()
                                        key_vals['品名_中文'] = alt_key_val
                                        break

                    if not key_vals:
                        continue

                    # 获取该行的数据值
                    row_values = {}
                    for field_name, field_col_idx in available_fields:
                        val = self._safe_get_value(row_data, field_col_idx)
                        if val:
                            row_values[field_name] = val

                    # 用每个有效的关联键建立映射
                    for key_type, key_val in key_vals.items():
                        if key_val not in key_field_values_map:
                            key_field_values_map[key_val] = {}
                        key_field_values_map[key_val].update(row_values)

                print(f"[多Sheet关联] Sheet '{other_sheet}' 建立 {len(key_field_values_map)} 个关联键映射")

                # 遍历主记录，用关联键匹配并补充字段
                matched_count = 0
                for record in main_records:
                    hscode = record.get('HSCODE', '')
                    product_name = record.get('品名', '')

                    # 优先用HSCODE匹配，其次用品名
                    key_to_use = None
                    if hscode and hscode in key_field_values_map:
                        key_to_use = hscode
                    elif product_name and product_name in key_field_values_map:
                        key_to_use = product_name

                    if key_to_use:
                        field_values = key_field_values_map[key_to_use]
                        for field_name, val in field_values.items():
                            # 材质需要特殊处理（提取成分含量）
                            if field_name == '材质':
                                val = self._extract_material_from_declaration(val)
                            # 只有当主记录中该字段为空时才补充
                            if not record.get(field_name):
                                record[field_name] = val
                                print(f"[多Sheet关联] 补充字段 {field_name}: '{key_to_use}' -> '{str(val)[:30]}...'")
                        matched_count += 1

                print(f"[多Sheet关联] Sheet '{other_sheet}' 匹配了 {matched_count} 条记录")

            # 检查材质是否提取到，如果没提取到，遍历所有涉及的Sheet扫描申报要素列
            material_extracted_count = sum(1 for r in main_records if r.get('材质'))
            if material_extracted_count == 0:
                print(f"[多Sheet关联] 材质未提取，遍历所有Sheet查找申报要素...")

                # 遍历所有涉及的Sheet，查找包含成分含量的列
                for search_sheet in involved_sheets:
                    sheet_df = sheets_data.get(search_sheet)
                    if sheet_df is None:
                        continue

                    sheet_struct = None
                    if data_map.sheet_structures and search_sheet in data_map.sheet_structures:
                        sheet_struct = data_map.sheet_structures[search_sheet]

                    if not sheet_struct:
                        continue

                    header_row = sheet_struct.get('header_row', 0)
                    sheet_field_cols = sheet_struct.get('field_cols', {})
                    data_start = sheet_struct.get('data_start_row', header_row + 1)

                    # 首先检查该Sheet的field_cols中是否已有材质列
                    material_col_idx = sheet_field_cols.get('材质')

                    if material_col_idx is None:
                        # 如果field_cols没有材质列，扫描所有数据行找包含"%"的列
                        material_col_idx = self._find_declaration_col(sheet_df, header_row, data_start)
                        if material_col_idx is not None:
                            print(f"[多Sheet关联] 在 Sheet '{search_sheet}' 通过扫描找到申报要素列: 列索引 {material_col_idx}")

                    if material_col_idx is None:
                        continue

                    print(f"[多Sheet关联] 从 Sheet '{search_sheet}' 提取材质，申报要素列: {material_col_idx}")

                    # 建立关联键到申报要素的映射
                    key_declaration_map = {}  # {key: declaration}

                    # 获取关联键列信息
                    hscode_col = sheet_field_cols.get('HSCODE')
                    product_col = sheet_field_cols.get('品名')
                    item_col = 0  # 假设项号在列0

                    # 首先，扫描所有行，建立 商品关键信息->申报要素 的映射
                    # 使用项号作为中间键来关联商品数据和申报要素
                    item_declaration_map = {}  # {item_no: declaration}
                    item_hscode_map = {}  # {item_no: hscode}
                    item_product_map = {}  # {item_no: product_name}

                    for row_idx in range(data_start, len(sheet_df)):
                        row_data = sheet_df.iloc[row_idx]
                        declaration = self._safe_get_value(row_data, material_col_idx)

                        # 检查是否是申报要素行（包含%或成分）
                        is_declaration_row = declaration and ('%' in declaration or '成分' in declaration)

                        if is_declaration_row:
                            # 申报要素行，找项号
                            item_no = self._safe_get_value(row_data, item_col)
                            if item_no:
                                item_declaration_map[item_no] = declaration
                        else:
                            # 商品数据行，提取项号、HSCODE、品名
                            item_no = self._safe_get_value(row_data, item_col)
                            hscode = self._safe_get_value(row_data, hscode_col) if hscode_col else None
                            product_name = self._safe_get_value(row_data, product_col) if product_col else None

                            if item_no:
                                if hscode:
                                    item_hscode_map[item_no] = str(hscode).strip()
                                if product_name:
                                    item_product_map[item_no] = str(product_name).strip()

                    # 用项号建立最终的映射：{hscode/product_name: declaration}
                    for item_no, declaration in item_declaration_map.items():
                        if item_no in item_hscode_map:
                            hscode = item_hscode_map[item_no]
                            key_declaration_map[hscode] = declaration
                        if item_no in item_product_map:
                            product_name = item_product_map[item_no]
                            key_declaration_map[product_name] = declaration
                        # 建立项号->HSCODE和项号->品名的映射
                        item_hscode_map = {}  # {item_no: hscode}
                        item_product_map = {}  # {item_no: product_name}

                        for row_idx in range(data_start, len(sheet_df)):
                            row_data = sheet_df.iloc[row_idx]
                            item_no = self._safe_get_value(row_data, 0)  # 列0通常是项号
                            hscode = self._safe_get_value(row_data, sheet_field_cols.get('HSCODE'))
                            product_name = self._safe_get_value(row_data, sheet_field_cols.get('品名'))

                            if item_no and hscode:
                                item_hscode_map[item_no] = hscode
                            if item_no and product_name:
                                item_product_map[item_no] = product_name

                        # 再扫描所有行，找申报要素行
                        for row_idx in range(data_start, len(sheet_df)):
                            row_data = sheet_df.iloc[row_idx]
                            declaration = self._safe_get_value(row_data, material_col_idx)

                            if declaration and ('%' in declaration or '成分' in declaration):
                                # 尝试找同行的项号
                                item_no = self._safe_get_value(row_data, 0)
                                if item_no and item_no in item_hscode_map:
                                    hscode = item_hscode_map[item_no]
                                    key_declaration_map[hscode] = declaration
                                if item_no and item_no in item_product_map:
                                    product_name = item_product_map[item_no]
                                    key_declaration_map[product_name] = declaration

                                # 如果同行没有项号，尝试上下邻近行
                                if not item_no or (item_no and item_no not in item_hscode_map):
                                    for offset in [-1, 1, -2, 2]:
                                        nearby_idx = row_idx + offset
                                        if 0 <= nearby_idx < len(sheet_df):
                                            nearby_row = sheet_df.iloc[nearby_idx]
                                            nearby_item = self._safe_get_value(nearby_row, 0)
                                            if nearby_item and nearby_item in item_hscode_map:
                                                hscode = item_hscode_map[nearby_item]
                                                key_declaration_map[hscode] = declaration
                                                break

                    if not key_declaration_map:
                        continue

                    print(f"[多Sheet关联] 从 Sheet '{search_sheet}' 建立 {len(key_declaration_map)} 个申报要素映射")

                    # 尝试用HSCODE或品名匹配提取材质
                    matched = 0
                    for record in main_records:
                        if record.get('材质'):
                            continue

                        hscode = record.get('HSCODE', '')
                        product_name = record.get('品名', '')

                        key_to_use = None
                        if hscode and hscode in key_declaration_map:
                            key_to_use = hscode
                        elif product_name and product_name in key_declaration_map:
                            key_to_use = product_name

                        if key_to_use:
                            raw_declaration = key_declaration_map[key_to_use]
                            material = self._extract_material_from_declaration(raw_declaration)
                            if material:
                                record['材质'] = material
                                matched += 1
                                print(f"[多Sheet关联] 从 Sheet '{search_sheet}' 提取材质: '{key_to_use}' -> '{material[:30]}...'")

                    if matched > 0:
                        print(f"[多Sheet关联] Sheet '{search_sheet}' 成功提取 {matched} 条材质")
                        break  # 找到材质后退出

            # 转换为 ProductRecord 列表
            for record in main_records:
                hscode = record.get('HSCODE', '')
                product_name = record.get('品名', '')
                quantity = record.get('件数', '')
                unit_price = record.get('单价', '')
                material = record.get('材质', '')

                all_records.append(ProductRecord(
                    hscode=hscode,
                    product_name=product_name,
                    quantity=quantity,
                    unit_price=unit_price,
                    material=material
                ))

            print(f"[多Sheet关联] 共提取 {len(all_records)} 条记录")

        except Exception as e:
            import traceback
            print(f"[错误] 多Sheet关联提取失败: {e}")
            print(f"[错误] traceback: {traceback.format_exc()}")

        return all_records

    def _extract_material_from_declaration(self, declaration: str) -> str:
        """
        从申报要素字符串中提取材质成分

        支持两种格式：
        1. 分号分隔格式: "针织;女式;90%锦纶10%氨纶"
        2. 键值对格式: "成分含量:(90%锦纶10%氨纶),品牌:(XXX)"

        例如：
        输入: "境内自主品牌;不享受;针织;女式;无领;无扣;无拉链;47%粘胶纤维+47%棉+6%氨纶;YUCOLEN牌"
        输出: "47%粘胶纤维+47%棉+6%氨纶"

        输入: "成分含量:(90%锦纶10%氨纶),品牌:(AVidlove)"
        输出: "90%锦纶10%氨纶"
        """
        if not declaration:
            return ""

        import re

        # 优先处理键值对格式（如"成分含量:(90%锦纶10%氨纶)"）
        # 匹配 "成分含量:" 后面括号内的内容
        bracket_match = re.search(r'成分含量[:：]\s*\(([^)]+)\)', declaration)
        if bracket_match:
            content = bracket_match.group(1).strip()
            # 验证括号内确实包含成分（数字+百分号）
            if '%' in content and re.search(r'\d+%', content):
                return content

        # 处理分号分隔格式
        # 匹配 XX%成分 的模式，如 "47%粘胶纤维" 或 "100%棉"
        # 成分可能是中文、英文或混合
        pattern = r'(\d+(?:\.\d+)?%[\u4e00-\u9fa5a-zA-Z]+(?:\s*[\+\/]\s*\d+(?:\.\d+)?%[\u4e00-\u9fa5a-zA]+)*)'
        matches = re.findall(pattern, declaration)

        if matches:
            # 返回第一个匹配的完整成分组合
            return matches[0]

        # 备选：直接返回声明中包含"%"的部分
        parts = declaration.split(';')
        for part in parts:
            if '%' in part and any(c.isdigit() for c in part):
                return part.strip()

        return declaration

    def _letter_to_col_index(self, col_letter: str) -> Optional[int]:
        """将Excel列名字母转换为列索引"""
        if not col_letter:
            return None
        col_letter = col_letter.upper()
        if len(col_letter) == 1:
            return ord(col_letter) - ord('A')
        elif len(col_letter) == 2:
            return (ord(col_letter[0]) - ord('A') + 1) * 26 + (ord(col_letter[1]) - ord('A'))
        else:
            return None


def main():
    """使用示例"""
    import os
    from pathlib import Path

    # 读取 .env 文件
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)
        print("[配置] 已加载 .env 文件")
    else:
        print("[警告] 未找到 .env 文件")

    # 配置 - 从环境变量读取或使用默认值
    API_KEY = os.environ.get("OPENAI_API_KEY", "")
    BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.minimaxi.com/v1")
    MODEL = os.environ.get("OPENAI_MODEL", "MiniMax-M2.7")

    if not API_KEY:
        print("[错误] 请在 .env 文件中设置 OPENAI_API_KEY")
        return

    EXCEL_FILE = os.environ.get("EXCEL_FILE", r"C:\Users\a1337\Downloads\1fXEQ8-a260315887+r1349000218.xlsx")
    OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "extracted_data123.xlsx")

    print(f"[配置] API: {BASE_URL}/{MODEL}")
    print(f"[配置] 输入: {EXCEL_FILE}")
    print(f"[配置] 输出: {OUTPUT_FILE}")

    # 创建提取器
    extractor = AutoGenExcelExtractor(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL
    )

     # 两阶段智能提取（自动分析 + 智能策略选择）
    records = extractor.extract_smart(
        file_path=EXCEL_FILE,
        output_path=OUTPUT_FILE,
        use_cache=True,           # 是否使用数据地图缓存
        confidence_threshold=0.8    # 置信度阈值
    )

    print(f"\n[结果] 共提取 {len(records)} 条产品记录")


if __name__ == "__main__":
    main()
