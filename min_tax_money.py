import numpy as np
import random

def calculate_min_tax(total_boxes, hs_data):
    """
    计算最小税金。

    Args:
        total_boxes (int): 总箱数。
        hs_data (list[dict]): 包含HS编码信息的列表，每个字典包含'hs_code', 'boxes' (已知箱数，未知则为None), 'tax_per_box'。
            例如: [{'hs_code': 'HS1', 'boxes': 10, 'tax_per_box': 2.5}, {'hs_code': 'HS2', 'boxes': None, 'tax_per_box': 1.5}]

    Returns:
        tuple: 包含最小税金和包含HS编码、箱数、单箱税金和总税金的字典的列表。
               如果无法找到有效解，返回 (None, None)。
    """

    # 分离已知和未知HS编码
    known_hs_boxes = {item['hs_code']: item['boxes'] for item in hs_data if item['boxes'] is not None}
    hs_tax_per_box = {item['hs_code']: item['tax_per_box'] for item in hs_data}
    unknown_hs = [item['hs_code'] for item in hs_data if item['boxes'] is None]
    num_unknown = len(unknown_hs)

    # 计算剩余箱数
    remaining_boxes = total_boxes - sum(known_hs_boxes.values())
    if remaining_boxes < 0:
        return None, None  # 如果已知箱数已经超过总箱数，则无解

    # 如果没有未知HS编码，则直接返回
    if num_unknown == 0:
        result = []
        total_tax = 0
        for item in hs_data:
            hs_code = item['hs_code']
            boxes = item['boxes']
            tax_per_box = item['tax_per_box']
            total_tax_for_hs = boxes * tax_per_box
            total_tax += total_tax_for_hs
            result.append({
                'hs_code': hs_code,
                'boxes': boxes,
                'tax_per_box': tax_per_box,
                'total_tax': total_tax_for_hs
            })
        return total_tax, result

    # 自定义初始箱数分配
    sorted_indices = np.argsort([hs_tax_per_box[unknown_hs[i]] for i in range(num_unknown)])
    initial_boxes = [0] * num_unknown
    
    # 税金最高的HS编码的初始箱数
    base_boxes = random.randint(10, 20)
    initial_boxes[sorted_indices[-1]] = base_boxes

    # 从高到低分配箱数
    for i in range(2, num_unknown + 1):
        if i == len(initial_boxes):
            initial_boxes[-1] = remaining_boxes - sum(initial_boxes)
            break
        if num_unknown - i >= 0:
            # base_boxes += random.randint(5, 10)
            initial_boxes[sorted_indices[-i]] = random.randint(15, 25)
    
    # 确保总箱数不超过剩余箱数，并进行调整
    total_initial_boxes = sum(initial_boxes)
    if total_initial_boxes > remaining_boxes:
        scale = remaining_boxes / total_initial_boxes
        initial_boxes = [int(box * scale) for box in initial_boxes]
        # 重新计算总数并进行调整
        total_initial_boxes = sum(initial_boxes)
        diff = remaining_boxes - total_initial_boxes
        for i in range(diff):
            initial_boxes[i % num_unknown] += 1

    unknown_hs_boxes = {}
    for i, hs_code in enumerate(unknown_hs):
        unknown_hs_boxes[hs_code] = initial_boxes[i]

    # 合并已知和未知箱数
    all_hs_boxes = known_hs_boxes.copy()
    all_hs_boxes.update(unknown_hs_boxes)

    # 计算总税金和结果列表
    total_tax = 0
    result_list = []
    for hs_code, boxes in all_hs_boxes.items():
        tax_per_box = hs_tax_per_box[hs_code]
        total_tax_for_hs = boxes * tax_per_box
        total_tax += total_tax_for_hs
        result_list.append({
            'hs_code': hs_code,
            'boxes': boxes,
            'tax_per_box': tax_per_box,
            'total_tax': total_tax_for_hs
        })

    return total_tax, result_list

if __name__ == '__main__':
    # 示例数据
    total_boxes = 974
    hs_data = [
    {'hs_code': '7007.19.0000', 'boxes': 53, 'tax_per_box': 4.26},
    {'hs_code': '8509.80.5095', 'boxes': 76, 'tax_per_box': 1.98},
    {'hs_code': '9506.99.3000', 'boxes': 33, 'tax_per_box': 7.38},
    {'hs_code': '8539.52.0091', 'boxes': 104, 'tax_per_box': 4.25},
    {'hs_code': '9006.91.0001', 'boxes': 171, 'tax_per_box': 0.36},
    {'hs_code': '8471.30.0100', 'boxes': 10, 'tax_per_box': 41.89},
    {'hs_code': '3926.90.9989', 'boxes': 20, 'tax_per_box': 0.64},
    {'hs_code': '6114.90.9040', 'boxes': None, 'tax_per_box': 5.47},
    {'hs_code': '6211.12.4000', 'boxes': None, 'tax_per_box': 6.91},
    {'hs_code': '6108.19.1000', 'boxes': None, 'tax_per_box': 15.56},
    {'hs_code': '6204.63.5000', 'boxes': None, 'tax_per_box': 6.47},
   
    {'hs_code': '6110.12.1020', 'boxes': None, 'tax_per_box': 4.66},
     {'hs_code': '6204.33.2000', 'boxes': None, 'tax_per_box': 3.59},
    ]

    # 计算最小税金
    min_tax, result = calculate_min_tax(total_boxes, hs_data)

    if min_tax is not None:
        print("最小税金:", min_tax)
        for item in result:
            print(f"HS编码: {item['hs_code']}, 箱数: {item['boxes']}, 单箱税金: {item['tax_per_box']}, 总税金: {item['total_tax']}")
    else:
        print("无法找到有效解。")
