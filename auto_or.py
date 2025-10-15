import json
from typing import Any, List, Dict, Optional

import pulp as lp
import httpx

# API配置 (保持不变)
API_URL = "http://47.103.138.130:8085/products/?get_all=true&username=admin&zishui=false&is_hidden=false"
API_TOKEN = ""  # 请手动填入你的token


def get_qingguan_access_token():
    with httpx.Client() as client:
        response = client.post(
            url="http://47.103.138.130:8085/login",
            json={"username": "admin", "password": "BG$6l6e*!5hj"},
        )
        api_token = response.json()["access_token"]

    return api_token


def fetch_products_from_api(
    api_url: str = API_URL, api_token: str = API_TOKEN
) -> Optional[List[Dict]]:
    """从API获取产品数据 (保持不变)"""
    try:
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

        with httpx.Client() as client:
            response = client.get(api_url, headers=headers)
            response.raise_for_status()

        api_data = response.json()
        print(f"✅ 成功从API获取到 {len(api_data)} 个产品数据")

        # 将API数据转换为所需格式
        api_products = []
        for item in api_data["items"]:
            try:
                # 获取基础税率
                base_duty = float(item.get("Duty", 0))

                # 获取加征税率并计算总和
                jiazeng_data = item.get("加征", {})
                jiazeng_total = sum(
                    float(value) for value in jiazeng_data.values() if value is not None
                )

                # 计算总税率：基础税率 + 所有加征税率总和
                total_tax_rate = base_duty + jiazeng_total

                product = {
                    "name": item.get("中文品名", ""),
                    "price": float(item.get("单价", 0)),
                    "pcs_per_box": int(item.get("件箱", 1)),
                    "tax_rate": total_tax_rate,
                    # **修改点**: 假设API现在提供重量范围
                    "min_weight_per_box": float(
                        item.get("min_weight", 0)
                    ),  # 示例：增加最小重量字段
                    "max_weight_per_box": float(
                        item.get("max_weight", 0)
                    ),  # 示例：增加最大重量字段
                }
                api_products.append(product)
            except Exception as e:
                print(f"[{ item.get('中文品名', '')}] 获取商品信息错误: {str(e)}")
                continue

        return api_products

    except httpx.RequestError as e:
        print(f"❌ 网络请求错误: {e}")
        return None
    except httpx.HTTPStatusError as e:
        print(f"❌ HTTP错误 {e.response.status_code}: {e}")
        return None
    except Exception as e:
        print(f"❌ 获取API数据时发生错误: {e}")
        return None


def optimize_packing_selection(
    products_data: List[Dict],  # 产品数据列表
    W_target: float = 3537,  # 目标总重量 (kg)
    B_target: int = 214,  # 目标总箱数
    alpha: float = 0.46,  # 货值/重量 最低比率 (美元/千克)
    beta_cny: float = 1.27,  # 税金/重量 最高比率 (人民币/千克)
    exchange_rate: float = 7.22,  # USD to CNY 汇率
    k: int = 3,  # 最多选择 k 个不同的产品品类
    min_boxes_per_product: int = 20,
    expansion_factor: int = None,
) -> Dict:
    """
    装箱选择优化函数 (已更新以处理重量范围)

    Args:
        products_data: 产品数据列表, 必须包含 name, price, pcs_per_box, tax_rate,
                       **min_weight_per_box**, **max_weight_per_box** 字段
        ... (其他参数不变)
    """
    if expansion_factor:
        alpha = alpha * expansion_factor
        beta_cny = beta_cny * expansion_factor
    products = products_data
    print(f"使用传入的产品数据进行优化，共 {len(products)} 个产品")
    if B_target > 0:
        avg_weight_per_box = W_target / B_target
        print(f"计算得出全局平均单箱重量: {avg_weight_per_box:.2f} kg")
        for p in products:
            # 使用 .get() 来安全地访问可能不存在的键
            # 如果 max_weight_per_box 是空的, None, 或者 0, 则视为无效
            if not p.get("max_weight_per_box") or p.get("max_weight_per_box") <= 0:
                print(
                    f"产品 '{p.get('name', '未知')}' 的重量范围为空或无效, 将使用平均单箱重量。"
                )
                p["min_weight_per_box"] = avg_weight_per_box
                p["max_weight_per_box"] = avg_weight_per_box
    # ======================
    # 数据预处理
    # ======================
    n: int = len(products)

    names: list = [p["name"] for p in products]
    unit_price_usd = [p["price"] for p in products]
    # 如果有膨胀系数，则膨胀价格
    if expansion_factor:
        unit_price_usd = [p * expansion_factor for p in unit_price_usd]

    pcs_per_box = [p["pcs_per_box"] for p in products]
    tax_rates = [p["tax_rate"] for p in products]
    # **修改点**: 获取重量范围
    min_weights = [p["min_weight_per_box"] for p in products]
    max_weights = [p["max_weight_per_box"] for p in products]

    value_per_box_usd = [unit_price_usd[i] * pcs_per_box[i] for i in range(n)]
    tax_per_box_usd = [value_per_box_usd[i] * tax_rates[i] for i in range(n)]
    tax_per_box_cny = [t * exchange_rate for t in tax_per_box_usd]

    # ======================
    # 建立优化模型
    # ======================
    model = lp.LpProblem("Packing_Selection_Optimization", lp.LpMinimize)

    # --- 决策变量 ---
    # b[i]: 产品i的箱数 (整数)
    b = lp.LpVariable.dicts("boxes", range(n), lowBound=0, cat="Integer")
    # z[i]: 是否选择产品i (二进制)
    z = lp.LpVariable.dicts("select", range(n), cat="Binary")
    # **新增变量**: w[i]: 产品i的总重量 (连续)
    w: dict = lp.LpVariable.dicts(
        "total_weight", range(n), lowBound=0, cat="Continuous"
    )

    # --- 目标函数：最小化总税金（CNY） ---
    model += (
        lp.lpSum([tax_per_box_cny[i] * b[i] for i in range(n)]),
        "Total_Tax_CNY",
    )

    # --- 约束条件 ---
    # 1. 总箱数约束
    model += lp.lpSum([b[i] for i in range(n)]) == B_target, "Total_Boxes"

    # 2. **修改点**: 总重量约束 (现在基于新的w变量)
    model += lp.lpSum([w[i] for i in range(n)]) == W_target, "Total_Weight"

    # 3. 货值/重量比率约束
    total_value_usd = lp.lpSum([value_per_box_usd[i] * b[i] for i in range(n)])
    model += total_value_usd >= alpha * W_target, "Min_Value_Weight_Ratio"

    # 4. 税金/重量比率约束
    if not expansion_factor:
        total_tax_cny = lp.lpSum([tax_per_box_cny[i] * b[i] for i in range(n)])
        model += total_tax_cny <= beta_cny * W_target, "Max_Tax_Weight_Ratio"

    # 5. 最多选择k个品类约束
    model += lp.lpSum([z[i] for i in range(n)]) == k, "Max_Product_Selection"

    # 6. 关联约束
    for i in range(n):
        # 如果选择一个产品 (z[i]=1)，其箱数b[i]必须大于等于最小箱数
        model += b[i] >= min_boxes_per_product * z[i], f"Min_Boxes_Constraint_{i}"
        # 如果不选择一个产品 (z[i]=0)，其箱数b[i]必须为0
        model += b[i] <= B_target * z[i], f"Link_Boxes_to_Selection_{i}"

        # 7. **新增约束**: 关联每个品类的总重量w[i]和箱数b[i]
        # 这是实现线性化的关键步骤
        model += w[i] >= min_weights[i] * b[i], f"Link_Min_Weight_{i}"
        model += w[i] <= max_weights[i] * b[i], f"Link_Max_Weight_{i}"

    # --- 求解模型 ---
    solver = lp.PULP_CBC_CMD(timeLimit=30)
    model.solve(solver)

    # ======================
    # 处理结果
    # ======================
    result = {
        "status": lp.LpStatus[model.status],
        "success": lp.LpStatus[model.status] == lp.LpStatus[1],
        "parameters": {
            "W_target": W_target,
            "B_target": B_target,
            "alpha": alpha,
            "beta_cny": beta_cny,
            "exchange_rate": exchange_rate,
            "k": k,
            "min_boxes_per_product": min_boxes_per_product,
        },
        "selected_products": [],
        "summary": {},
    }

    print(f"状态: {lp.LpStatus[model.status]}")
    if lp.LpStatus[model.status] == lp.LpStatus[1]:  # Optimal
        print(f"\n✅ 优化成功！总箱数={B_target}, 总重量={W_target}kg\n")
        print(f"要求：货值/重量 ≥ {alpha:.2f} USD/kg, 税金/重量 ≤ {beta_cny:.2f} CNY/kg")
        print(f"汇率: 1 USD = {exchange_rate} CNY，最多选 {k} 个品类")
        print("-" * 105)

        total_weight_check = 0
        total_value_usd_check = 0
        total_tax_cny_check = 0
        selected_count = 0

        print(
            f"{'品名':<15} : {'箱数':>4} | {'总重(kg)':>9} | {'单箱重(kg)':>10} | "
            f"{'货值(USD)':>10} | {'税金(CNY)':>9} | {'总件数':>7}"
        )
        print("-" * 105)

        for i in range(n):
            boxes_count = b[i].varValue
            if boxes_count > 0:
                selected_count += 1
                # 从结果中获取每个品类的总重量
                total_w = w[i].varValue
                # 计算出实际的单箱重量
                avg_weight_per_box = total_w / boxes_count if boxes_count > 0 else 0

                val_usd = value_per_box_usd[i] * boxes_count
                tax_cny = tax_per_box_cny[i] * boxes_count
                pcs = pcs_per_box[i] * boxes_count

                total_weight_check += total_w
                total_value_usd_check += val_usd
                total_tax_cny_check += tax_cny

                product_result = {
                    "name": names[i],
                    "boxes": int(boxes_count),
                    "weight_per_box": avg_weight_per_box,
                    "total_weight": total_w,
                    "value_usd": val_usd,
                    "tax_cny": tax_cny,
                    "pieces_per_box": pcs_per_box[i],
                    "pieces": int(pcs),
                }
                result["selected_products"].append(product_result)

                print(
                    f"{names[i]:<15} : {boxes_count:4.0f} | {total_w:9.2f} | {avg_weight_per_box:10.2f} | "
                    f"{val_usd:10.2f} | {tax_cny:9.2f} | {pcs:7.0f}"
                )
        print("-" * 105)
        print(
            f"总计: {B_target:4.0f} | {total_weight_check:9.2f} | {' ':>10} | "
            f"{total_value_usd_check:10.2f} | {total_tax_cny_check:9.2f} |"
        )

        # 验证比率
        value_per_weight_usd = total_value_usd_check / total_weight_check
        tax_per_weight_cny = total_tax_cny_check / total_weight_check
        value_ratio_ok = value_per_weight_usd >= alpha
        tax_ratio_ok = tax_per_weight_cny <= beta_cny

        print("\n--- 结果验证 ---")
        print(
            f"货值/重量 = {value_per_weight_usd:.2f} USD/kg (要求 ≥ {alpha}) {'✅' if value_ratio_ok else '❌'}"
        )
        if expansion_factor:
            print(
                f"有膨胀系数，不需要验证单位税金, 税金/重量 = {tax_per_weight_cny:.2f} CNY/kg ✅"
            )
        else:
            print(
                f"税金/重量 = {tax_per_weight_cny:.2f} CNY/kg (要求 ≤ {beta_cny}) {'✅' if tax_ratio_ok else '❌'}"
            )
        allGoodsPrice = total_value_usd_check
        mpf_result = allGoodsPrice * 0.003464
        mpf_min_total = 33.58 if mpf_result < 33.58 else (634.62 if mpf_result > 634.62 else mpf_result)
        result["summary"] = {
            "total_weight": total_weight_check,
            "total_value_usd": total_value_usd_check,
            "total_tax_cny": total_tax_cny_check ,
            "selected_count": selected_count,
            "value_per_weight_usd": value_per_weight_usd,
            "tax_per_weight_cny": tax_per_weight_cny,
            "value_ratio_ok": value_ratio_ok,
            "tax_ratio_ok": tax_ratio_ok,
            'MPF':mpf_min_total
        }

    else:
        print("❌ 未能找到可行解，请检查约束是否过于严格。")
    # print(json.dumps(result, indent=4))
    return result


if __name__ == "__main__":
    # 更新了示例数据，加入了单箱重量范围
    upload_data = {
        "products_data": [
            {
                "name": "衬裙/婚礼裙",
                "price": 0.7,
                "pcs_per_box": 18,
                "tax_rate": 0.31100000000000005,
                "single_weight": 0,
                "min_weight_per_box": 8,
                "max_weight_per_box": 10,
            },
            {
                "name": "睡裙/睡袍 高税",
                "price": 0.64,
                "pcs_per_box": 17,
                "tax_rate": 0.338,
                "single_weight": 0,
                "min_weight_per_box": 10,
                "max_weight_per_box": 20,
            },
            {
                "name": "睡裙/睡袍",
                "price": 0.61,
                "pcs_per_box": 17,
                "tax_rate": 0.31100000000000005,
                "single_weight": 0,
                "min_weight_per_box": 5,
                "max_weight_per_box": 10,
            },
            {
                "name": "男士体恤/上衣",
                "price": 0.7,
                "pcs_per_box": 17,
                "tax_rate": 0.32600000000000007,
                "single_weight": 0,
                "min_weight_per_box": 4,
                "max_weight_per_box": 6,
            },
            {
                "name": "女士体恤/上衣",
                "price": 0.75,
                "pcs_per_box": 18,
                "tax_rate": 0.32600000000000007,
                "single_weight": 0,
                "min_weight_per_box": 7,
                "max_weight_per_box": 9,
            },
        ],
        "W_target": 3537,
        "B_target": 214,
        "alpha": 0.5,
        "beta_cny": 1.26,
        "exchange_rate": 7.22,
        "k": 3,
        "min_boxes_per_product": 20,
        "expansion_factor": 2,
    }
    products_data = upload_data["products_data"]
    result = optimize_packing_selection(
        products_data=products_data,
        W_target=upload_data["W_target"],
        B_target=upload_data["B_target"],
        alpha=upload_data["alpha"],
        beta_cny=upload_data["beta_cny"],
        exchange_rate=upload_data["exchange_rate"],
        k=upload_data["k"],
        min_boxes_per_product=upload_data["min_boxes_per_product"],
        expansion_factor=upload_data["expansion_factor"],
    )
    print(result)
