import json
import random
from typing import Any, Counter, List, Dict, Optional

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
) -> Dict:
    """
    装箱选择优化函数，支持原始约束失败后自动切换到宽松约束（alpha=0.1, beta=20）
    """
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

            # 数据准备
            names = [p["name"] for p in products_data]
            unit_prices = [
                p["price"] * (expansion_factor or 1.0) for p in products_data
            ]
            pcs_per_box = [p["pcs_per_box"] for p in products_data]
            tax_rates = [p["tax_rate"] for p in products_data]

            # 重量范围处理
            avg_w_box = W_target / B_target if B_target > 0 else 0
            min_weights, max_weights = [], []
            for p in products_data:
                mi, ma = p.get("min_weight_per_box", 0), p.get("max_weight_per_box", 0)
                if not ma or ma <= 0:
                    mi, ma = avg_w_box - 7, avg_w_box + 7
                min_weights.append(mi)
                max_weights.append(ma)

            # 随机化重量以避开局部最优
            dups = {val for val, count in Counter(min_weights).items() if count > 1}
            min_weights = [
                mw + random.uniform(0.1, 2.0) if mw in dups else mw
                for mw in min_weights
            ]

            val_per_box = [unit_prices[i] * pcs_per_box[i] for i in range(n)]
            tax_per_box = [
                val_per_box[i] * tax_rates[i] * exchange_rate for i in range(n)
            ]

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
                model += b[i] >= min_boxes_per_product * z[i]
                model += b[i] <= B_target * z[i]
                model += w[i] >= min_weights[i] * b[i]
                model += w[i] <= max_weights[i] * b[i]

            # 求解
            model.solve(lp.PULP_CBC_CMD(timeLimit=30, msg=0))

            if lp.LpStatus[model.status] == "Optimal":
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
                                "total_weight": wv,
                                "value_usd": vv,
                                "tax_cny": tc,
                                "pieces_per_box": pcs_per_box[i],
                                "pieces": bc * pcs_per_box[i],
                            }
                        )
                        tw += wv
                        tv += vv
                        tt += tc

                results.append(
                    {
                        "selected_products": selected,
                        "summary": {
                            "total_weight": tw,
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
                    f"{p['name']:<25} | {p['boxes']:>5} | {p['total_weight']:>10.1f} | {p['value_usd']:>12.1f} | {p['tax_cny']:>10.1f}"
                )
            print("-" * 85)
            print(
                f"总计: {B_target:>22} | {s['total_weight']:>10.1f} | {s['total_value_usd']:>12.1f} | {s['total_tax_cny']:>10.1f}"
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
