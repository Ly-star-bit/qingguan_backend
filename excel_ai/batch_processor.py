"""
批量处理目录中的所有Excel文件
使用AI驱动的提取器提取数据
"""

import sys
import os
from pathlib import Path

# 设置 UTF-8 编码
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv

# 读取 .env 文件
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    load_dotenv(env_file)
    print("[配置] 已加载 .env 文件")

from autogen_extractor import AutoGenExcelExtractor


def process_directory(input_dir: str, output_dir: str = None, use_cache: bool = True):
    """
    处理目录中的所有Excel文件

    Args:
        input_dir: 输入目录路径
        output_dir: 输出目录路径（默认为输入目录下的 output 文件夹）
        use_cache: 是否使用缓存
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir else input_path / "output"

    # 创建输出目录
    output_path.mkdir(exist_ok=True)
    print(f"[配置] 输入目录: {input_path.absolute()}")
    print(f"[配置] 输出目录: {output_path.absolute()}")

    # 获取所有 Excel 文件
    excel_files = list(input_path.glob("*.xlsx")) + list(input_path.glob("*.xls"))
    excel_files = [f for f in excel_files if not f.name.startswith("~$")]  # 排除临时文件

    if not excel_files:
        print(f"[错误] 目录 '{input_dir}' 中没有找到 Excel 文件")
        return

    print(f"[信息] 找到 {len(excel_files)} 个 Excel 文件:")
    for f in excel_files:
        print(f"  - {f.name}")

    # 读取配置
    API_KEY = os.environ.get("OPENAI_API_KEY", "")
    BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.minimaxi.com/v1")
    MODEL = os.environ.get("OPENAI_MODEL", "MiniMax-M2.7")

    if not API_KEY:
        print("[错误] 请在 .env 文件中设置 OPENAI_API_KEY")
        return

    # 创建提取器
    extractor = AutoGenExcelExtractor(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL
    )

    # 批量处理
    total_records = 0
    success_count = 0
    fail_count = 0

    for excel_file in excel_files:
        print(f"\n{'='*60}")
        print(f"[处理] 开始处理: {excel_file.name}")
        print(f"{'='*60}")

        try:
            # 输出文件路径
            output_file = output_path / f"extracted_{excel_file.name}"

            # 执行提取
            records = extractor.extract_smart(
                file_path=str(excel_file.absolute()),
                output_path=str(output_file.absolute()),
                use_cache=use_cache,
                confidence_threshold=0.8
            )

            print(f"[完成] {excel_file.name}: 提取到 {len(records)} 条记录")
            print(f"[保存] 输出到: {output_file}")

            total_records += len(records)
            success_count += 1

        except Exception as e:
            print(f"[错误] 处理 '{excel_file.name}' 失败: {e}")
            fail_count += 1

    # 打印汇总
    print(f"\n{'='*60}")
    print(f"[汇总] 处理完成")
    print(f"{'='*60}")
    print(f"  总文件数: {len(excel_files)}")
    print(f"  成功: {success_count}")
    print(f"  失败: {fail_count}")
    print(f"  总记录数: {total_records}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="批量处理Excel文件")
    parser.add_argument(
        "-i", "--input",
        default=None,
        help="输入目录路径（默认：当前目录下的 excel_process_fiel）"
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="输出目录路径（默认：输入目录下的 output）"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="禁用缓存，强制重新分析"
    )

    args = parser.parse_args()

    # 默认输入目录
    if args.input is None:
        script_dir = Path(__file__).parent
        input_dir = script_dir / "excel_process_fiel"
    else:
        input_dir = Path(args.input)

    process_directory(
        input_dir=str(input_dir),
        output_dir=args.output,
        use_cache=not args.no_cache
    )


if __name__ == "__main__":
    main()
