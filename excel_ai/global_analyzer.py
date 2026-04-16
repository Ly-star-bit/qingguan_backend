"""
全局分析器 - 两阶段智能提取方案第一阶段
读取所有 Sheet 结构，调用 AI 生成数据地图
"""

import sys
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# 设置 UTF-8 编码
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
import openai

from .models import DataMap


class GlobalAnalyzer:
    """
    全局分析器 - 分析 Excel 结构并生成数据地图

    职责：
    1. 读取所有 Sheet 前3行获取表头
    2. 调用 AI 生成数据地图（字段分布 + 关联关系 + 提取策略）
    3. 返回 DataMap 对象
    """

    SYSTEM_PROMPT = """你是一个专业的Excel数据结构分析专家。

你的职责：
1. 分析多个Sheet的列名和数据样本
2. 识别Sheet之间的关联关系（基于相似列名、主键、外键等）
3. 确定最佳数据提取策略（单Sheet提取或多Sheet关联提取）

关键概念：
- data_distribution: 每个目标字段所在的sheet/column/提取方法
- relationships: Sheet间的关联关系（如 HSCODE 列作为关联键）
- extraction_strategy:
  - "single_sheet": 数据全部在一个Sheet中
  - "multi_sheet_join": 数据分散在多个Sheet，需要关联提取

输出要求：
请以JSON格式输出数据地图，包含以下字段：
```json
{
  "data_distribution": {
    "HSCODE": {"sheet": "Sheet1", "column": "B", "extract_method": "direct"},
    "品名": {"sheet": "Sheet1", "column": "C", "extract_method": "direct"},
    "件数": {"sheet": "Sheet2", "column": "D", "extract_method": "direct"},
    "单价": {"sheet": "Sheet1", "column": "E", "extract_method": "direct"},
    "材质": {"sheet": "Sheet1", "column": "F", "extract_method": "regex", "pattern": "成分含量[：:]\s*([^）)]+)"}
  },
  "relationships": [
    {"sheet1": "Sheet1", "sheet2": "Sheet2", "key_column": "HSCODE", "relationship": "one_to_many"}
  ],
  "extraction_strategy": "single_sheet" 或 "multi_sheet_join",
  "confidence": 0.95
}
```

请开始分析。"""

    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1", model: str = "gpt-4o"):
        """
        初始化全局分析器

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

    def _read_sheets_header(self, excel_file: str) -> Dict[str, Any]:
        """
        读取所有 Sheet 的完整数据（逐块扫描整个sheet）

        策略：读取整个sheet，按10行一块让AI分析，找到表头位置和各列索引
        使用并发处理多个Sheet加速分析

        Returns:
            Dict[sheet_name, {columns: [...], sample_data: [...], structure: {...}}]
        """
        sheets_structure = {}

        try:
            xl = pd.ExcelFile(excel_file)
            sheet_names = xl.sheet_names
            print(f"[全局分析] 发现 {len(sheet_names)} 个 Sheet")

            # 用于存储每个sheet的数据
            sheet_data_map = {}  # {sheet_name: df_raw}

            # 首先读取所有Sheet的数据
            for name in sheet_names:
                try:
                    df_raw = pd.read_excel(xl, sheet_name=name, header=None)
                    sheet_data_map[name] = df_raw
                except Exception as e:
                    print(f"[警告] 无法读取 Sheet '{name}': {e}")

            # 定义处理单个sheet的函数
            def process_sheet(name: str, df_raw: pd.DataFrame):
                try:
                    total_rows = len(df_raw)
                    total_cols = len(df_raw.columns)

                    print(f"[全局分析] Sheet '{name}': {total_rows} 行 x {total_cols} 列，开始逐块分析结构")

                    # 逐块分析（每10行为一块），让AI找到表头和数据区域
                    chunk_size = 10
                    all_chunks_data = []

                    for start in range(0, total_rows, chunk_size):
                        end = min(start + chunk_size, total_rows)
                        chunk = df_raw.iloc[start:end]

                        # 转换为可序列化格式
                        chunk_records = []
                        for i in range(len(chunk)):
                            row_dict = {}
                            for j in range(len(chunk.columns)):
                                val = chunk.iloc[i, j]
                                if pd.notna(val):
                                    row_dict[str(j)] = str(val)
                            chunk_records.append(row_dict)

                        all_chunks_data.append({
                            "row_range": f"{start}-{end-1}",
                            "data": chunk_records
                        })

                    print(f"[全局分析] Sheet '{name}': 扫描完成，共 {len(all_chunks_data)} 个数据块")

                    # 串行分析块数据（避免瞬时并发过高导致限流）
                    structure_info = self._analyze_structure_with_ai(name, all_chunks_data, total_rows, total_cols)

                    result = {
                        "columns": list(df_raw.columns),
                        "sample_data": all_chunks_data,
                        "structure": structure_info
                    }
                    print(f"[全局分析] Sheet '{name}': 结构分析完成 - {structure_info}")
                    return (name, result)
                except Exception as e:
                    import traceback
                    print(f"[警告] 处理 Sheet '{name}' 失败: {e}")
                    return (name, None)

            # 使用并发处理所有Sheet
            max_workers = min(4, len(sheet_data_map))  # 最多4个并发
            print(f"[全局分析] 使用 {max_workers} 个并发线程分析 Sheet")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_sheet, name, df_raw): name
                          for name, df_raw in sheet_data_map.items()}

                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        if result is not None:
                            sheet_name, data = result
                            if data is not None:
                                sheets_structure[sheet_name] = data
                    except Exception as e:
                        print(f"[警告] Sheet '{name}' 并发处理异常: {e}")

        except Exception as e:
            print(f"[错误] 无法读取 Excel 文件: {e}")

        return sheets_structure

    def _make_serializable(self, obj):
        """
        将对象转换为 JSON 可序列化的格式
        处理 datetime、date 等类型转换为字符串
        """
        import datetime as dt
        if isinstance(obj, (dt.datetime, dt.date, dt.time)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, float) and (obj != obj):  # NaN check
            return None
        elif isinstance(obj, float) and obj == float('inf'):
            return None
        elif isinstance(obj, float) and obj == float('-inf'):
            return None
        return obj

    def _analyze_structure_with_ai(self, sheet_name: str, chunks_data: List[Dict], total_rows: int, total_cols: int) -> Dict:
        """
        使用AI分析Sheet结构（逐块扫描方式）

        让AI看10行一块的块数据，找出：
        1. 表头在哪一行
        2. 各目标字段（HSCODE、品名、件数、单价、材质）对应的列索引
        3. 数据从哪一行开始

        Args:
            sheet_name: Sheet名称
            chunks_data: 10行一块的数据
            total_rows: 总行数
            total_cols: 总列数

        Returns:
            结构信息字典
        """
        # 构建prompt，让AI分析块数据
        prompt_parts = [
            f"分析 Sheet '{sheet_name}' 的结构：\n",
            f"总行数: {total_rows}, 总列数: {total_cols}\n",
            f"数据按10行/块组织（列名为数字索引0,1,2...）：\n\n"
        ]

        for chunk in chunks_data:
            row_range = chunk["row_range"]
            data = chunk["data"]
            prompt_parts.append(f"--- 行{row_range} ---\n")
            for row in data:
                prompt_parts.append(f"  {row}\n")

        prompt_parts.append("\n请分析以上数据，找出表头行和商品相关字段：\n")
        prompt_parts.append("1. 表头行是第几行？\n")
        prompt_parts.append("2. 商品字段的列索引（如果不存在该字段则写null）：\n")
        prompt_parts.append("   - HSCODE：海关编码，10位纯数字（如没有则null）\n")
        prompt_parts.append("   - 品名：商品名称，可能叫'Description'、'品名'、'商品名称'等\n")
        prompt_parts.append("   - 件数：数量/件数，可能叫'Quantity'、'件数'、'数量'等\n")
        prompt_parts.append("   - 单价：单价，可能叫'Unit Price'、'单价'等\n")
        prompt_parts.append("   - 材质：成分含量（如没有则null）\n")
        prompt_parts.append("3. 数据从第几行开始？\n\n")
        prompt_parts.append("严格按以下JSON格式输出（不得附加任何文字）：\n")
        prompt_parts.append('{"header_row": 数字, "field_cols": {"HSCODE": 数字或null, "品名": 数字或null, "件数": 数字或null, "单价": 数字或null, "材质": 数字或null}, "data_start_row": 数字}')

        user_prompt = "".join(prompt_parts)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是Excel结构分析专家。只输出JSON格式的结果，不要输出任何思考过程、解释或其他文字。输出格式：{\"header_row\": 数字, \"field_cols\": {\"字段名\": 列索引, ...}, \"data_start_row\": 数字}"},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0,
                max_tokens=4096
            )

            ai_response = response.choices[0].message.content
            print(f"[全局分析] Sheet '{sheet_name}' AI结构分析返回: {ai_response[:500]}")

            # 解析AI返回的结构
            structure = self._parse_structure_from_ai(ai_response)
            return structure

        except Exception as e:
            import traceback
            print(f"[全局分析] AI结构分析失败: {e}")
            print(f"[全局分析] traceback: {traceback.format_exc()}")
            return {
                "header_row": 0,
                "field_cols": {},
                "data_start_row": 1,
                "notes": f"分析失败: {str(e)}"
            }

    def _parse_structure_from_ai(self, ai_response: str) -> Dict:
        """解析AI返回的结构分析结果"""
        import re

        # 清理思考标记
        cleaned = ai_response
        # 移除 <think>...</think> 思考过程
        cleaned = re.sub(r'</think>.*?</think>', '', cleaned, flags=re.DOTALL)
        # 移除普通解释性文字，只保留最后的 JSON 部分
        cleaned = cleaned.strip()

        # 从后往前找 JSON 对象
        last_bracket_pos = cleaned.rfind('}')
        attempts = 0
        while last_bracket_pos > 0 and attempts < 100:
            attempts += 1
            start_pos = cleaned.rfind('{', 0, last_bracket_pos)
            if start_pos < 0:
                break
            candidate = cleaned[start_pos:last_bracket_pos + 1]
            try:
                data = json.loads(candidate)
                if isinstance(data, dict) and 'field_cols' in data:
                    field_cols = data.get('field_cols', {})
                    # 确保 field_cols 中的值是整数或None
                    cleaned_field_cols = {}
                    for k, v in field_cols.items():
                        if v is None:
                            cleaned_field_cols[k] = None
                        elif isinstance(v, str) and v.lower() == 'null':
                            cleaned_field_cols[k] = None
                        elif isinstance(v, str) and v.isdigit():
                            cleaned_field_cols[k] = int(v)
                        elif isinstance(v, int):
                            cleaned_field_cols[k] = v
                        else:
                            try:
                                cleaned_field_cols[k] = int(v)
                            except:
                                cleaned_field_cols[k] = None
                    field_cols = cleaned_field_cols
                    print(f"[全局分析] 结构解析成功: header_row={data.get('header_row')}, fields={list(field_cols.keys())}")
                    return {
                        "header_row": data.get('header_row', 0),
                        "field_cols": field_cols,
                        "data_start_row": data.get('data_start_row', 1)
                    }
            except json.JSONDecodeError:
                pass
            last_bracket_pos = cleaned.rfind('}', 0, last_bracket_pos - 1)

        # 尝试用正则直接匹配 JSON 中的关键字段
        try:
            match = re.search(r'"header_row"\s*:\s*(\d+)', cleaned)
            if match:
                header_row = int(match.group(1))
                # 尝试提取 field_cols
                field_cols_match = re.search(r'"field_cols"\s*:\s*\{([^}]+)\}', cleaned)
                if field_cols_match:
                    field_cols_str = field_cols_match.group(1)
                    field_cols = {}
                    # 解析 field_cols
                    for kv_match in re.finditer(r'"(\w+)"\s*:\s*(\d+)', field_cols_str):
                        field_name = kv_match.group(1)
                        col_idx = int(kv_match.group(2))
                        field_cols[field_name] = col_idx
                    if field_cols:
                        data_start_match = re.search(r'"data_start_row"\s*:\s*(\d+)', cleaned)
                        data_start_row = int(data_start_match.group(1)) if data_start_match else header_row + 1
                        print(f"[全局分析] 正则提取成功: header_row={header_row}, fields={list(field_cols.keys())}")
                        return {
                            "header_row": header_row,
                            "field_cols": field_cols,
                            "data_start_row": data_start_row
                        }
        except Exception as e:
            print(f"[全局分析] 正则提取失败: {e}")

        print(f"[全局分析] AI返回无法解析: {cleaned[:300]}")
        return {
            "header_row": 0,
            "field_cols": {},
            "data_start_row": 1,
            "notes": "解析失败"
        }

    def _col_index_to_letter(self, col_idx: int) -> str:
        """
        将列索引转换为 Excel 列名

        Args:
            col_idx: 列索引（0-based）

        Returns:
            Excel 列名（如 0->A, 1->B, 26->AA）
        """
        if col_idx < 26:
            return chr(ord('A') + col_idx)
        elif col_idx < 52:
            return 'A' + chr(ord('A') + col_idx - 26)
        elif col_idx < 702:
            # 两位字母
            first = chr(ord('A') + (col_idx // 26) - 1)
            second = chr(ord('A') + col_idx % 26)
            return first + second
        else:
            # 三位字母（理论上不太可能用到）
            first = chr(ord('A') + (col_idx // 676) - 1)
            second = chr(ord('A') + (col_idx // 26) % 26)
            third = chr(ord('A') + col_idx % 26)
            return first + second + third

    def _build_analysis_prompt(self, sheets_structure: Dict[str, Any]) -> str:
        """
        构建 AI 分析 prompt

        Args:
            sheets_structure: 各 Sheet 的结构信息

        Returns:
            格式化的 prompt 字符串
        """
        prompt_parts = ["请分析以下 Excel 的多 Sheet 结构，生成数据地图：\n"]

        for sheet_name, info in sheets_structure.items():
            prompt_parts.append(f"\n## Sheet: {sheet_name}")
            prompt_parts.append(f"列名: {info['columns']}")
            prompt_parts.append(f"样本数据（前{len(info['sample_data'])}行）:")
            for i, row in enumerate(info['sample_data']):
                prompt_parts.append(f"  行{i+1}: {row}")

        prompt_parts.append("\n\n需要提取的5个目标字段：")
        prompt_parts.append("1. HSCODE - 海关编码（通常10位数字）")
        prompt_parts.append("2. 品名 - 商品名称")
        prompt_parts.append("3. 件数 - 数量/件数")
        prompt_parts.append("4. 单价 - 单价")
        prompt_parts.append("5. 材质 - 从申报要素中的成分含量提取")

        prompt_parts.append("\n\n请输出JSON格式的数据地图分析结果：")

        return "".join(prompt_parts)

    def analyze_excel_structure(self, excel_file: str) -> Optional[DataMap]:
        """
        分析 Excel 文件结构并生成数据地图

        直接使用 _read_sheets_header 中每个 Sheet 的 AI 分析结果，
        转换为 DataMap 格式返回。不再做额外的 AI 调用。

        Args:
            excel_file: Excel 文件路径

        Returns:
            DataMap 对象，失败返回 None
        """
        excel_file_abs = str(Path(excel_file).resolve())
        print(f"[全局分析] 开始分析: {excel_file_abs}")

        # Step 1: 读取所有 Sheet 的数据，并让 AI 分析每个 Sheet 的结构
        sheets_structure = self._read_sheets_header(excel_file_abs)

        if not sheets_structure:
            print("[错误] 未能读取任何 Sheet 数据")
            return None

        # Step 2: 直接使用每个 Sheet 的 AI 分析结果，转换为 DataMap 格式
        data_distribution = {}
        sheet_structures = {}
        extraction_strategy = "single_sheet"  # 默认单 Sheet

        for sheet_name, info in sheets_structure.items():
            structure = info.get('structure', {})
            field_cols = structure.get('field_cols', {})

            if not field_cols:
                print(f"[警告] Sheet '{sheet_name}' 没有找到字段分布信息")
                continue

            header_row = structure.get('header_row', 0)
            data_start_row = structure.get('data_start_row', header_row + 1)

            # 存储每个 Sheet 的详细结构
            sheet_structures[sheet_name] = {
                "header_row": header_row,
                "field_cols": field_cols,
                "data_start_row": data_start_row
            }

            # 将列索引转换为 Excel 列名（如 1 -> "B"）
            # 对于每个字段，选择实际有该字段的第一个sheet
            for field_name, col_idx in field_cols.items():
                # 如果字段已存在，不覆盖（保持第一个找到的）
                if field_name in data_distribution:
                    continue

                col_letter = self._col_index_to_letter(col_idx)
                data_distribution[field_name] = {
                    "sheet": sheet_name,
                    "column": col_letter,
                    "extract_method": "direct"
                }

        # 判断是单 Sheet 还是多 Sheet
        sheets_in_distribution = set(v.get("sheet") for v in data_distribution.values())
        if len(sheets_in_distribution) > 1:
            extraction_strategy = "multi_sheet_join"

        if not data_distribution:
            print("[错误] 未能从任何 Sheet 中提取字段分布信息")
            return None

        # 计算置信度：基于找到的字段数量
        required_fields = ['品名', '件数', '单价']  # 核心字段
        optional_fields = ['HSCODE', '材质']  # 可选字段
        found_required = sum(1 for f in required_fields if f in data_distribution)
        found_optional = sum(1 for f in optional_fields if f in data_distribution)

        # 置信度 = 核心字段权重(70%) + 可选字段权重(30%)
        confidence = (found_required / len(required_fields)) * 0.7 + (found_optional / len(optional_fields)) * 0.3

        # 构建 DataMap
        data_map = DataMap()
        data_map.data_distribution = data_distribution
        data_map.sheet_structures = sheet_structures
        data_map.extraction_strategy = extraction_strategy
        data_map.confidence = confidence
        data_map.relationships = []

        print(f"[全局分析] 成功生成数据地图")
        print(f"[全局分析] 提取策略: {data_map.extraction_strategy}")
        print(f"[全局分析] 置信度: {data_map.confidence}")
        print(f"[全局分析] 字段分布: {data_map.data_distribution}")
        print(f"[全局分析] Sheet结构: {data_map.sheet_structures}")

        return data_map


# 测试代码
if __name__ == "__main__":
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    # 读取 .env 文件
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        print("[配置] 已加载 .env 文件")

    API_KEY = os.environ.get("OPENAI_API_KEY", "")
    BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.minimaxi.com/v1")
    MODEL = os.environ.get("OPENAI_MODEL", "MiniMax-M2.7")

    if not API_KEY:
        print("[错误] 请设置 OPENAI_API_KEY")
    else:
        EXCEL_FILE = os.environ.get("EXCEL_FILE", r"C:\Users\a1337\Documents\xwechat_files\wxid_v64vfmjwbj8b22_3be2\msg\file\2026-03\进出口文件-GQ888260311277\拉森-260311-04-xmls26257 35\XMLS26257-报关资料-35箱.xlsx")

        analyzer = GlobalAnalyzer(api_key=API_KEY, base_url=BASE_URL, model=MODEL)
        data_map = analyzer.analyze_excel_structure(EXCEL_FILE)

        if data_map:
            print(f"\n[成功] 数据地图生成完成")
            print(f"[策略] {data_map.extraction_strategy}")
            print(f"[分布] {data_map.data_distribution}")
