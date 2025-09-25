import io
import json
import os
import re
import tempfile

import PyPDF2
import pandas as pd

def extract_zip_codes_from_pdf(pdf_path):
    """
    从PDF文件中提取邮政编码
    
    Args:
        pdf_path: PDF文件路径
    
    Returns:
        dict: 以标识为键，邮政编码列表为值的字典
    """
    # 检查是否存在缓存文件
    cache_file = pdf_path + '.json'
    if os.path.exists(cache_file):
        print(f"从缓存文件加载邮政编码数据: {cache_file}")
        with open(cache_file, 'r') as f:
            return json.load(f)
    
    print(f"从PDF文件提取邮政编码数据: {pdf_path}")
    # 打开PDF文件
    pdf_file = open(pdf_path, 'rb')
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    
    # 存储结果的字典
    zip_codes_by_category = {}
    current_category = None
    
    # 遍历PDF的每一页
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        
        # 检查页面是否包含标识
        category_match = re.search(r'Deliver y Area Surcharge ZIP codes:\s*(.+?)(?:\n|$)', text)
        
        if category_match:
            # 提取类别名称并清理
            category = category_match.group(1).strip()
            
            # 处理续页情况 (cont.)
            if '(cont.)' in category:
                # 移除(cont.)部分，使用基本类别名称
                base_category = category.replace('(cont.)', '').strip()
                current_category = base_category
            else:
                current_category = category
            
            # 如果是新类别，初始化列表
            if current_category not in zip_codes_by_category:
                zip_codes_by_category[current_category] = []
            
            # 提取邮政编码
            # 使用正则表达式匹配邮政编码格式
            zip_codes = re.findall(r'\b\d{5}(?:-\d{5})?\b', text)
            
            # 将提取的邮政编码添加到当前类别
            zip_codes_by_category[current_category].extend(zip_codes)
    
    # 关闭PDF文件
    pdf_file.close()
    
    # 将结果保存到缓存文件
    with open(cache_file, 'w') as f:
        json.dump(zip_codes_by_category, f)
    
    return zip_codes_by_category
def get_ups_zip_data_from_file(file_content: bytes):
    """
    从UPS的Excel文件内容中提取邮政编码和其属性的映射关系。
    
    Args:
        file_content: Excel文件的字节内容。
    
    Returns:
        dict: 以属性为键，邮政编码列表为值的字典。
    """
    property_excel_path = io.BytesIO(file_content)
    xl = pd.ExcelFile(property_excel_path)
    code_property_map = {}
    
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(property_excel_path, sheet_name=sheet_name)
        data = []
        for col in df.columns:
            for cell in df[col].dropna():
                cell_str = str(cell).zfill(5)
                # 使用正则表达式提取数字
                codes = re.findall(r'\b\d+\b', cell_str)
                for code in codes:
                    if code == '00000':
                        continue
                    data.append(code)
    
        code_property_map[sheet_name] = data
    return code_property_map


def get_fedex_zip_data_from_file(file_content: bytes):
    """
    从FedEx的PDF文件内容中提取邮政编码和其分类的映射关系。
    
    Args:
        file_content: PDF文件的字节内容。
    
    Returns:
        dict: 以分类为键，邮政编码列表为值的字典。
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
        temp_pdf.write(file_content)
        temp_pdf_path = temp_pdf.name
    
    try:
        zip_data = extract_zip_codes_from_pdf(temp_pdf_path)
    finally:
        os.remove(temp_pdf_path)
        
        # 移除可能生成的缓存文件
        cache_file = temp_pdf_path + '.json'
        if os.path.exists(cache_file):
            os.remove(cache_file)
            
    return zip_data


def compare_zip_data(data1: dict, data2: dict):
    """
    比较两个从文件中提取的邮政编码数据字典。
    
    Args:
        data1: 第一个数据字典。
        data2: 第二个数据字典。
    
    Returns:
        dict: 包含差异信息的字典。
    """
    data1_keys = set(data1.keys())
    data2_keys = set(data2.keys())
    
    added_categories = list(data2_keys - data1_keys)
    removed_categories = list(data1_keys - data2_keys)
    
    changed_categories = {}
    common_keys = data1_keys.intersection(data2_keys)
    
    for key in common_keys:
        set1 = set(data1[key])
        set2 = set(data2[key])
        
        added_zips = list(set2 - set1)
        removed_zips = list(set1 - set2)
        
        if added_zips or removed_zips:
            changed_categories[key] = {
                "added": added_zips,
                "removed": removed_zips
            }
            
    return {
        "added_categories": added_categories,
        "removed_categories": removed_categories,
        "changed_categories": changed_categories
    }


async def compare_ups_zip_files(file1, file2):
    """
    比较两个UPS邮政编码定义文件。
    
    Args:
        file1: 第一个（旧的）UPS Excel文件。
        file2: 第二个（新的）UPS Excel文件。
        
    Returns:
        dict: 比较结果。
    """
    content1 = await file1.read()
    content2 = await file2.read()
    
    data1 = get_ups_zip_data_from_file(content1)
    data2 = get_ups_zip_data_from_file(content2)
    
    return compare_zip_data(data1, data2)

async def compare_fedex_zip_files(file1, file2):
    """
    比较两个FedEx邮政编码定义文件。
    
    Args:
        file1: 第一个（旧的）FedEx PDF文件。
        file2: 第二个（新的）FedEx PDF文件。
        
    Returns:
        dict: 比较结果。
    """
    content1 = await file1.read()
    content2 = await file2.read()

    data1 = get_fedex_zip_data_from_file(content1)
    data2 = get_fedex_zip_data_from_file(content2)
    
    return compare_zip_data(data1, data2)