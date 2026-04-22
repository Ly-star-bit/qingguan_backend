import json
import random
import math
from typing import Any, Counter, List, Dict, Optional, Tuple

import pulp as lp
import httpx

# API配置模板 (包含起始地、目的地和运输类型参数)
API_URL_TEMPLATE = "http://47.103.138.130:8085/qingguan/{transport_endpoint}?get_all=true&username=admin&zishui=false&is_hidden=false&startland={startland}&destination={destination}"
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
    api_url: str = None,
    api_token: str = API_TOKEN,
    startland: str = "China",
    destination: str = "America",
    transport_type: str = "air",
) -> Optional[List[Dict]]:
    """从API获取产品数据 (包含起始地和目的地参数)"""
    try:
        # 如果没有提供 api_url，则根据运输类型构建URL
        if api_url is None:
            # 根据运输类型选择端点
            endpoint = "products" if transport_type.lower() == "air" else "products_sea"
            api_url = API_URL_TEMPLATE.format(
                transport_endpoint=endpoint,
                startland=startland,
                destination=destination,
            )

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

        print(
            f"🔗 正在从API获取产品数据，运输类型: {transport_type}, 起始地: {startland}, 目的地: {destination}"
        )

        with httpx.Client(follow_redirects=True) as client:
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
                    "min_weight_per_box": float(item.get("min_weight", 0)),
                    "max_weight_per_box": float(item.get("max_weight", 0)),
                }
                api_products.append(product)
            except Exception as e:
                print(f"[{item.get('中文品名', '')}] 获取商品信息错误: {str(e)}")
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
from decimal import Decimal, ROUND_HALF_UP

def process_unit_price(value):
    # 1. 处理空值或非数字情况
    if value is None or value == "":
        return Decimal(0)  # 统一返回 Decimal 类型
    
    try:
        # 转换为 Decimal 以保证精度
        # 如果输入是字符串，直接转；如果是 float，先转字符串再转 Decimal 避免浮点误差
        if isinstance(value, float):
            num = Decimal(str(value))
        else:
            num = Decimal(str(value))
            
        # 第一步：标准四舍五入保留两位小数
        # quantize(Decimal('0.01')) 配合 ROUND_HALF_UP 实现标准的四舍五入
        rounded_num = num.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        # 获取当前两位小数的数值 (0 到 99)
        # 方法：将数字乘以 100 取整
        cents = int(rounded_num * 100)
        last_digit = cents % 10  # 获取第二位小数 (0-9)
        
        new_last_digit = 0
        
        # 第二步：应用特殊规则
        if 1 <= last_digit <= 5:
            new_last_digit = 0
        elif 6 <= last_digit <= 9:
            new_last_digit = 5
        else:
            # 如果是 0，保持不变
            new_last_digit = 0
            
        # 重新构建数值
        # 去掉原来的个位小数，加上新的个位小数
        base_cents = cents - last_digit + new_last_digit
        
        final_num = Decimal(base_cents) / 100
                # 如果转换后为0，保持原始值
        if final_num == 0:
            return f"{num:.2f}"
        # 修改点：直接返回 Decimal 数字对象，不再格式化为字符串
        return final_num
        
    except Exception:
        # 如果转换失败（例如原本就是非数字字符串），返回 Decimal(0)
        return Decimal(0)

def optimize_packing_selection(
    products_data: List[Dict],
    W_target: float = 3537,
    B_target: int = 214,
    alpha: float = 0.46,
    beta_cny: float = 1.27,
    exchange_rate: float = 7.22,
    k: int = 3,
    min_boxes_per_product: int = 20,
    expansion_factor: int = None,
    max_attempts: int = 5,
    min_box_ratio_range: Tuple[float, float] = (0.10, 0.15),
    max_box_ratio_range: Tuple[float, float] = (0.60, 0.65),
) -> Dict:
    """
    装箱选择优化函数，支持原始约束失败后自动切换到宽松约束（alpha=0.1, beta=20）

    新增约束：
    - min_box_ratio_range: 最小箱数产品的箱数占比范围 (默认 10%-15%)
    - max_box_ratio_range: 最大箱数产品的箱数占比范围 (默认 60%-65%)
    """
    min_ratio, max_ratio = min_box_ratio_range[0], min_box_ratio_range[1]
    max_ratio_upper = max_box_ratio_range[1]
    if expansion_factor:
        alpha *= expansion_factor
        beta_cny *= expansion_factor

    if not products_data:
        print("❌ 错误：产品数据为空")
        return {
            "status": "Failed",
            "success": False,
            "error": "产品数据为空",
            "selected_products": [],
            "summary": {},
        }

    # 优化配置：先尝试原始参数，失败后尝试宽松参数
    trial_configs = [
        {"alpha": alpha, "beta": beta_cny, "is_relaxed": False, "label": "原始参数"},
        {"alpha": 0.1, "beta": 20.0, "is_relaxed": True, "label": "宽松参数"},
    ]

    for cfg in trial_configs:
        curr_alpha, curr_beta, is_relaxed = cfg["alpha"], cfg["beta"], cfg["is_relaxed"]

        if is_relaxed:
            print(f"\n{'='*80}")
            print(
                f"⚠️  警告：原始参数下无可行解 (alpha={alpha:.2f}, beta_cny={beta_cny:.2f})"
            )
            print(
                f"🔄 正在切换到宽松参数重新尝试: alpha={curr_alpha}, beta_cny={curr_beta}"
            )
            print(f"{'='*80}\n")
        else:
            print(
                f"\n开始使用原始参数进行优化 (alpha={curr_alpha:.2f}, beta_cny={curr_beta:.2f})..."
            )

        results = []
        for attempt in range(max_attempts):
            # 每次尝试随机生成固定的最小/最大箱数占比
            curr_min_ratio = random.uniform(min_ratio, max_ratio)  # 如 0.12
            curr_max_ratio = random.uniform(max_box_ratio_range[0], max_box_ratio_range[1])  # 如 0.62
            print(f"  尝试 {attempt+1}: curr_min_ratio={curr_min_ratio:.3f}, curr_max_ratio={curr_max_ratio:.3f}, 最小箱数范围=[{curr_min_ratio:.3f}, {curr_min_ratio+0.03:.3f}], 最大箱数范围=[{curr_max_ratio-0.03:.3f}, {curr_max_ratio:.3f}]")

            # 建立优化模型
            model = lp.LpProblem(
                f"Packing_{'Relaxed' if is_relaxed else 'Original'}_{attempt+1}",
                lp.LpMinimize,
            )
            n = len(products_data)

            # 决策变量
            b = lp.LpVariable.dicts("boxes", range(n), lowBound=0, cat="Integer")
            z = lp.LpVariable.dicts("select", range(n), cat="Binary")
            w = lp.LpVariable.dicts("weight", range(n), lowBound=0, cat="Continuous")

            # 排序约束变量：产品i是否被选中且箱数在所有选中产品中最小/最大
            is_min = lp.LpVariable.dicts("is_min", range(n), cat="Binary")
            is_max = lp.LpVariable.dicts("is_max", range(n), cat="Binary")

            # 数据准备
            names = [p["name"] for p in products_data]
            unit_prices = [
                process_unit_price(Decimal(str(p["price"])) * Decimal(str(expansion_factor or 1.0))) for p in products_data
            ]
            pcs_per_box = [p["pcs_per_box"] for p in products_data]
            tax_rates = [p["tax_rate"] for p in products_data]

            # 重量范围处理
            avg_w_box = W_target / B_target if B_target > 0 else 0
            min_weights, max_weights, min_boxes_list, target_boxes_list = [], [], [], []
            for p in products_data:
                mi, ma = p.get("min_weight_per_box", 0), p.get("max_weight_per_box", 0)
                if not ma or ma <= 0:
                    mi, ma = avg_w_box - 7, avg_w_box + 7
                min_weights.append(mi)
                max_weights.append(ma)
                # 每个产品单独的 min_boxes，没有则使用全局 min_boxes_per_product
                min_boxes_list.append(p.get("min_boxes") if p.get("min_boxes") is not None else min_boxes_per_product)
                # target_boxes：如果有且不为空，则箱数固定
                target_boxes_list.append(p.get("target_boxes"))

            # 随机化重量以避开局部最优
            dups = {val for val, count in Counter(min_weights).items() if count > 1}
            min_weights = [
                mw + random.uniform(0.1, 2.0) if mw in dups else mw
                for mw in min_weights
            ]

            val_per_box = [unit_prices[i] * pcs_per_box[i] for i in range(n)]
            tax_per_box = [
                float(val_per_box[i]) * float(Decimal(str(tax_rates[i]))) * float(Decimal(str(exchange_rate))) for i in range(n)
            ]
            val_per_box = [float(v) for v in val_per_box]

            # 目标函数与约束
            model += lp.lpSum([tax_per_box[i] * b[i] for i in range(n)])
            model += lp.lpSum([b[i] for i in range(n)]) == B_target
            model += lp.lpSum([w[i] for i in range(n)]) == W_target
            model += (
                lp.lpSum([val_per_box[i] * b[i] for i in range(n)])
                >= curr_alpha * W_target
            )
            model += (
                lp.lpSum([tax_per_box[i] * b[i] for i in range(n)])
                <= curr_beta * W_target
            )
            model += lp.lpSum([z[i] for i in range(n)]) == k

            for i in range(n):
                if target_boxes_list[i] is not None and target_boxes_list[i] > 0:
                    # 固定箱数的产品
                    model += b[i] == target_boxes_list[i] * z[i]
                else:
                    # 非固定箱数，使用 min_boxes 约束
                    model += b[i] >= min_boxes_list[i] * z[i]
                    model += b[i] <= B_target * z[i]
                model += w[i] >= min_weights[i] * b[i]
                model += w[i] <= max_weights[i] * b[i]

            # ========== 暂时移除排序约束，只保留基本的 alpha/beta 约束 ==========
            # 排序约束已移除，让优化器自由选择
            # 如果后续需要识别最小/最大产品，可以在阶段2后处理中完成

            # 求解
            model.solve(lp.PULP_CBC_CMD(timeLimit=30, msg=0))

            if lp.LpStatus[model.status] == "Optimal":
                print(f"    ✅ 尝试 {attempt+1} 成功找到一个可行解！")
                selected = []
                tw, tv, tt = 0, 0, 0
                for i in range(n):
                    if b[i].varValue > 0:
                        bc = int(b[i].varValue)
                        wv = w[i].varValue
                        vv = val_per_box[i] * bc
                        tc = tax_per_box[i] * bc
                        selected.append(
                            {
                                "name": names[i],
                                "boxes": bc,
                                "weight_per_box": wv / bc,
                                "total_weight": round(wv),
                                "value_usd": vv,
                                "tax_cny": tc,
                                "pieces_per_box": pcs_per_box[i],
                                "pieces": bc * pcs_per_box[i],
                                # 存储用于后续调整
                                "_val_per_box": val_per_box[i],
                                "_tax_per_box": tax_per_box[i],
                                "_original_index": i,
                            }
                        )
                        tw += wv
                        tv += vv
                        tt += tc

                results.append(
                    {
                        "selected_products": selected,
                        "summary": {
                            "total_weight": round(tw),
                            "total_value_usd": tv,
                            "total_tax_cny": tt,
                            "value_per_weight_usd": tv / tw if tw > 0 else 0,
                            "tax_per_weight_cny": tt / tw if tw > 0 else 0,
                            "selected_count": len(selected),
                            "is_relaxed": is_relaxed,
                            "status_message": "⚠️ 原始参数失败，当前使用宽松参数 (alpha=0.1, beta=20)"
                            if is_relaxed
                            else "✅ 成功满足原始约束条件",
                        },
                    }
                )

        if results:
            # 选择最优结果
            best = max(
                results,
                key=lambda x: (
                    x["summary"]["value_per_weight_usd"],
                    -x["summary"]["tax_per_weight_cny"],
                ),
            )
            s = best["summary"]
            s["MPF"] = max(33.58, min(634.62, s["total_value_usd"] * 0.003464))

            # ========== 阶段2：后处理百分比约束 ==========
            # 按箱数排序，找出最小和最大的产品
            sorted_by_boxes = sorted(best["selected_products"], key=lambda x: x["boxes"])
            total_boxes = B_target

            if len(sorted_by_boxes) >= 2:
                # 最小箱数产品
                min_product = sorted_by_boxes[0]
                min_ratio = min_product["boxes"] / total_boxes
                # 最大箱数产品
                max_product = sorted_by_boxes[-1]
                max_ratio = max_product["boxes"] / total_boxes

                print(f"\n阶段2 - 箱数分布检查:")
                print(f"  最小箱数产品: {min_product['name']} - {min_product['boxes']}箱 (占比{min_ratio:.1%})")
                print(f"  最大箱数产品: {max_product['name']} - {max_product['boxes']}箱 (占比{max_ratio:.1%})")

                # 检查是否满足百分比约束
                min_ok = min_box_ratio_range[0] <= min_ratio <= min_box_ratio_range[1]
                max_ok = max_box_ratio_range[0] <= max_ratio <= max_box_ratio_range[1]

                print(f"  最小箱数约束 [{min_box_ratio_range[0]:.1%}, {min_box_ratio_range[1]:.1%}]: {'✅' if min_ok else '❌'} ({min_ratio:.1%})")
                print(f"  最大箱数约束 [{max_box_ratio_range[0]:.1%}, {max_box_ratio_range[1]:.1%}]: {'✅' if max_ok else '❌'} ({max_ratio:.1%})")

                # 如果不满足，尝试调整
                if not min_ok or not max_ok:
                    # 在范围内随机选择目标比例
                    target_min_ratio = random.uniform(min_box_ratio_range[0], min_box_ratio_range[1])
                    target_max_ratio = random.uniform(max_box_ratio_range[0], max_box_ratio_range[1])
                    target_min_boxes = int(B_target * target_min_ratio)
                    target_max_boxes = int(B_target * target_max_ratio)

                    print(f"\n  正在调整箱数...")
                    print(f"  随机目标: 最小={target_min_boxes}箱 ({target_min_ratio:.1%}), 最大={target_max_boxes}箱 ({target_max_ratio:.1%})")

                    # 计算调整前的总箱数
                    old_total_boxes = sum(p["boxes"] for p in best["selected_products"])

                    # 调整最小箱数产品
                    if not min_ok and min_ratio < min_box_ratio_range[0]:
                        min_product["boxes"] = target_min_boxes
                    elif not min_ok and min_ratio > min_box_ratio_range[1]:
                        min_product["boxes"] = max(target_min_boxes, 1)

                    # 调整最大箱数产品
                    if not max_ok and max_ratio < max_box_ratio_range[0]:
                        max_product["boxes"] = target_max_boxes
                    elif not max_ok and max_ratio > max_box_ratio_range[1]:
                        max_product["boxes"] = min(target_max_boxes, B_target - k + 1)

                    # 计算调整后的总箱数
                    new_total_boxes = sum(p["boxes"] for p in best["selected_products"])
                    box_diff = new_total_boxes - old_total_boxes

                    # 如果总箱数不等于 B_target，需要调整其他产品
                    if box_diff != 0:
                        # 找出非最小/非最大的产品
                        other_products = [p for p in best["selected_products"] if p != min_product and p != max_product]
                        if other_products and box_diff < 0:
                            # 需要减少箱数，从其他产品均匀减少
                            reduce_per_product = abs(box_diff) // len(other_products)
                            for p in other_products:
                                orig_idx = p["_original_index"]
                                min_limit = min_boxes_list[orig_idx]
                                p["boxes"] = max(p["boxes"] - reduce_per_product, min_limit)
                        elif other_products and box_diff > 0:
                            # 需要增加箱数，向其他产品均匀增加
                            add_per_product = box_diff // len(other_products)
                            for p in other_products:
                                p["boxes"] = min(p["boxes"] + add_per_product, B_target)

                        # 重新计算确保总箱数 = B_target
                        current_total = sum(p["boxes"] for p in best["selected_products"])
                        if current_total != B_target and other_products:
                            # 调整第一个其他产品来弥补差值
                            if current_total < B_target:
                                other_products[0]["boxes"] += (B_target - current_total)
                            elif current_total > B_target:
                                orig_idx = other_products[0]["_original_index"]
                                min_limit = min_boxes_list[orig_idx]
                                other_products[0]["boxes"] = max(other_products[0]["boxes"] - (current_total - B_target), min_limit)

                    # 重新计算每个产品的重量、货值、税金
                    for p in best["selected_products"]:
                        p["pieces"] = p["boxes"] * p["pieces_per_box"]
                        # 重新计算货值和税金（基于调整后的箱数）
                        p["value_usd"] = p["_val_per_box"] * p["boxes"]
                        p["tax_cny"] = p["_tax_per_box"] * p["boxes"]

                    # 重新计算汇总
                    tw = sum(p["total_weight"] for p in best["selected_products"])
                    tv = sum(p["value_usd"] for p in best["selected_products"])
                    tt = sum(p["tax_cny"] for p in best["selected_products"])

                    # 如果总重量偏差超过5%，尝试微调weight_per_box来修正
                    weight_diff = tw - W_target
                    if abs(weight_diff) > W_target * 0.05 and k >= 2:
                        # 通过调整中间产品的weight_per_box来修正总重量
                        # 找到中间产品（非最小非最大）
                        mid_products = [p for p in best["selected_products"] if p != min_product and p != max_product]
                        if mid_products:
                            # 计算每个产品的调整量
                            adjust_per_box = weight_diff / mid_products[0]["boxes"]
                            for p in mid_products:
                                new_wpb = p["weight_per_box"] - adjust_per_box
                                # 确保在允许范围内
                                orig_idx = p["_original_index"]
                                if min_weights[orig_idx] <= new_wpb <= max_weights[orig_idx]:
                                    p["weight_per_box"] = new_wpb
                                    break
                            # 重新计算重量
                            for p in best["selected_products"]:
                                p["total_weight"] = round(p["weight_per_box"] * p["boxes"])
                            tw = sum(p["total_weight"] for p in best["selected_products"])

                    s["total_weight"] = round(tw)
                    s["total_value_usd"] = tv
                    s["total_tax_cny"] = tt
                    s["tax_per_weight_cny"] = tt / tw if tw > 0 else 0

                    # 重新计算 MPF
                    s["MPF"] = max(33.58, min(634.62, tv * 0.003464))

                    # 验证最终占比
                    final_sorted = sorted(best["selected_products"], key=lambda x: x["boxes"])
                    final_min_ratio = final_sorted[0]["boxes"] / B_target
                    final_max_ratio = final_sorted[-1]["boxes"] / B_target

                    print(f"  调整后: 最小={final_sorted[0]['boxes']}箱 ({final_min_ratio:.1%}), 最大={final_sorted[-1]['boxes']}箱 ({final_max_ratio:.1%})")
                    print(f"  调整后汇总: 总重={tw}kg (目标{W_target}kg, 偏差{tw-W_target:+.0f}kg), 货值={tv}USD, 税金={tt}CNY")

            # 打印优化报表
            print("\n" + "=" * 85)
            print(f"【优化报表】 {s['status_message']}")
            print(f"使用参数: alpha={curr_alpha:.2f}, beta_cny={curr_beta:.2f}")
            print("-" * 85)
            print(
                f"{'品名':<25} | {'箱数':>5} | {'总重(kg)':>10} | {'货值(USD)':>12} | {'税金(CNY)':>10}"
            )
            for p in best["selected_products"]:
                print(
                    f"{p['name']:<25} | {p['boxes']:>5} | {p['total_weight']:>10} | {p['value_usd']:>12.1f} | {p['tax_cny']:>10.1f}"
                )
            print("-" * 85)
            print(
                f"总计: {B_target:>22} | {s['total_weight']:>10} | {s['total_value_usd']:>12.1f} | {s['total_tax_cny']:>10.1f}"
            )
            print(
                f"验证指标: 货值/重量={s['value_per_weight_usd']:.2f} (阈值≥{curr_alpha:.2f}) | 税金/重量={s['tax_per_weight_cny']:.2f} (阈值≤{curr_beta:.2f})"
            )
            print("=" * 85 + "\n")

            return {
                "status": "Optimal",
                "success": True,
                "is_relaxed": is_relaxed,
                "selected_products": best["selected_products"],
                "summary": s,
                "parameters": {
                    "W_target": W_target,
                    "B_target": B_target,
                    "alpha": curr_alpha,
                    "beta_cny": curr_beta,
                    "original_alpha": alpha if is_relaxed else curr_alpha,
                    "original_beta_cny": beta_cny if is_relaxed else curr_beta,
                    "exchange_rate": exchange_rate,
                    "k": k,
                    "min_boxes_per_product": min_boxes_per_product,
                    "min_box_ratio_range": min_box_ratio_range,
                    "max_box_ratio_range": max_box_ratio_range,
                },
            }

    print("\n❌ 遗憾：即使使用宽松参数也无法在当前数据下找到可行解。")
    return {
        "status": "Failed",
        "success": False,
        "error": "所有参数配置下均无解",
        "selected_products": [],
        "summary": {},
    }


if __name__ == "__main__":
    # 测试数据
    upload_data = {
        "products_data": [
            {
                "name": "女士体恤/上衣",
                "price": 0.75,
                "pcs_per_box": 18,
                "tax_rate": 0.226,
                "single_weight": 0,
                "min_weight_per_box": 0,
                "max_weight_per_box": 0,
            },
            {
                "name": "手机/电脑支架支架-非磁力",
                "price": 0.3,
                "pcs_per_box": 16,
                "tax_rate": 0.253,
                "single_weight": 0,
                "min_weight_per_box": 0,
                "max_weight_per_box": 0,
            },
        ],
        "W_target": 2107,
        "B_target": 193,
        "alpha": 0.35,
        "beta_cny": 5,
        "exchange_rate": 7.21,
        "k": 2,
        "min_boxes_per_product": 20,
        "expansion_factor": None,
        "startland": "China",
        "destination": "America",
        "transport_type": "air",
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
        max_attempts=5,
    )
    print(result)
