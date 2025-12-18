#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel数据处理脚本
功能：
1. 处理第一个Excel文件的订单编号（前向填充空值）
2. 根据用户单号匹配订单编号，提取SKU和库存SKU英文名称
3. 支持一个订单号对应多个产品的情况
4. 自动计算并填充商品数量总和
"""

import pandas as pd
import os
from typing import Dict, List, Tuple


def process_order_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理订单编号列：空值时使用上一行的订单编号
    前提是该行有数据（至少有一个非空列）

    Args:
        df: 包含订单数据的DataFrame

    Returns:
        处理后的DataFrame
    """
    df = df.copy()

    # 确保订单编号列存在
    if '订单编号' not in df.columns:
        raise ValueError("第一个Excel文件中未找到'订单编号'列")

    # 前向填充订单编号，但只填充有数据的行
    prev_order_num = None
    for idx, row in df.iterrows():
        # 检查当前行是否有数据（至少有一个非空值）
        row_has_data = row.notna().any()

        if row_has_data:
            if pd.notna(row['订单编号']):
                # 当前行有订单编号，更新prev_order_num
                prev_order_num = row['订单编号']
            elif prev_order_num is not None:
                # 当前行没有订单编号但有其他数据，使用上一行的订单编号
                df.at[idx, '订单编号'] = prev_order_num

    return df


def find_matching_products(file1_path: str, file2_path: str,
                          output_path: str = None) -> pd.DataFrame:
    """
    匹配两个Excel文件的数据并提取SKU信息

    Args:
        file1_path: 第一个Excel文件路径（包含订单编号、SKU、库存SKU英文名称、商品数量）
        file2_path: 第二个Excel文件路径（包含用户单号）
        output_path: 输出文件路径（可选）

    Returns:
        处理后的第二个DataFrame，包含SKU、product列和商品数量总和
    """
    # 读取Excel文件
    print(f"正在读取文件: {file1_path}")
    df1 = pd.read_excel(file1_path, sheet_name=0)

    print(f"正在读取文件: {file2_path}")
    df2 = pd.read_excel(file2_path, sheet_name=0)

    # 检查必要列是否存在
    required_cols_df1 = ['订单编号', 'SKU', '库存SKU英文名称', '商品数量']
    for col in required_cols_df1:
        if col not in df1.columns:
            raise ValueError(f"第一个Excel文件中未找到'{col}'列")

    if '用户单号' not in df2.columns:
        raise ValueError("第二个Excel文件中未找到'用户单号'列")

    # 确保第二个文件有商品数量列
    if '商品数量' not in df2.columns:
        df2['商品数量'] = 0

    # 处理第一个文件的订单编号
    print("正在处理订单编号（前向填充空值）...")
    df1 = process_order_numbers(df1)

    # 创建订单编号到SKU信息的映射
    print("正在构建订单编号到SKU的映射...")
    order_to_skus = {}
    for order_num in df1['订单编号'].unique():
        if pd.notna(order_num):
            matching_rows = df1[df1['订单编号'] == order_num]
            skus = []
            product_names = []
            total_quantity = 0  # 累计商品数量

            for _, row in matching_rows.iterrows():
                if pd.notna(row['SKU']):
                    skus.append(str(row['SKU']))
                if pd.notna(row['库存SKU英文名称']):
                    product_names.append(str(row['库存SKU英文名称']))
                # 累加商品数量
                if pd.notna(row['商品数量']):
                    try:
                        total_quantity += float(row['商品数量'])
                    except (ValueError, TypeError):
                        # 如果无法转换为数字，跳过
                        pass

            order_to_skus[order_num] = {
                'skus': skus,
                'product_names': product_names,
                'total_quantity': total_quantity
            }

    # 处理第二个文件的用户单号
    print("正在处理用户单号并匹配订单...")
    # 确保SKU和product列存在
    if 'SKU' not in df2.columns:
        df2['SKU'] = ''

    # 查找最大可能的产品列数
    max_products = 0
    for user_order_num in df2['用户单号']:
        if pd.notna(user_order_num) and str(user_order_num) in order_to_skus:
            product_count = len(order_to_skus[str(user_order_num)]['product_names'])
            max_products = max(max_products, product_count)

    # 创建product列（品名列）
    for i in range(1, max_products + 1):
        col_name = f'品名{i}'
        if col_name not in df2.columns:
            df2[col_name] = ''

    # 填充数据
    for idx, row in df2.iterrows():
        user_order_num = row['用户单号']

        if pd.notna(user_order_num) and str(user_order_num) in order_to_skus:
            order_data = order_to_skus[str(user_order_num)]

            # 设置SKU（逗号合并）
            if order_data['skus']:
                df2.at[idx, 'SKU'] = ','.join(order_data['skus'])
            else:
                df2.at[idx, 'SKU'] = ''

            # 设置商品数量（求和）
            df2.at[idx, '商品数量'] = order_data['total_quantity']

            # 设置product列（品名列）
            for i, product_name in enumerate(order_data['product_names']):
                col_name = f'品名{i + 1}'
                df2.at[idx, col_name] = product_name
        else:
            # 未找到匹配的订单
            df2.at[idx, 'SKU'] = ''
            df2.at[idx, '商品数量'] = 0
            for i in range(1, max_products + 1):
                col_name = f'品名{i}'
                df2.at[idx, col_name] = ''

    # 保存结果
    if output_path:
        print(f"正在保存结果到: {output_path}")
        df2.to_excel(output_path, index=False)
        print("保存完成！")

    return df2


def main():
    """
    主函数：演示如何使用该脚本
    """
    # 示例用法
    file1_path = r"C:\Users\a1337\Desktop\100箱的商品名称跟订单数量.xlsx"  # 第一个Excel文件路径
    file2_path = r"C:\Users\a1337\Desktop\17号100箱明细.xlsx"  # 第二个Excel文件路径
    output_path = "处理结果.xlsx"  # 输出文件路径

    print("=" * 60)
    print("Excel数据处理脚本")
    print("=" * 60)

    try:
        # 检查文件是否存在
        if not os.path.exists(file1_path):
            print(f"警告: 文件不存在 {file1_path}")
            print("请确保文件路径正确，或修改脚本中的文件路径")

        if not os.path.exists(file2_path):
            print(f"警告: 文件不存在 {file2_path}")
            print("请确保文件路径正确，或修改脚本中的文件路径")

        if os.path.exists(file1_path) and os.path.exists(file2_path):
            # 执行处理
            result_df = find_matching_products(file1_path, file2_path, output_path)

            print("\n处理完成！")
            print(f"结果已保存到: {output_path}")
            print(f"处理了 {len(result_df)} 行数据")

            # 显示前5行结果
            print("\n前5行处理结果预览:")
            print(result_df.head())

    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()