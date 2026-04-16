"""
数据地图缓存 - 缓存 GlobalAnalyzer 生成的数据地图
避免重复分析同一文件
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Optional

# 缓存目录
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)


class DataMapCache:
    """数据地图缓存管理器"""

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or CACHE_DIR

    def _get_cache_path(self, file_path: str) -> Path:
        """根据文件路径生成缓存文件路径"""
        # 使用文件路径的 MD5 作为缓存文件名
        file_hash = hashlib.md5(file_path.encode()).hexdigest()
        return self.cache_dir / f"datamap_{file_hash}.json"

    def get(self, file_path: str) -> Optional[dict]:
        """
        从缓存获取数据地图

        Args:
            file_path: Excel 文件路径

        Returns:
            DataMap 字典，如果缓存不存在或无效则返回 None
        """
        cache_path = self._get_cache_path(file_path)

        if not cache_path.exists():
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"[缓存] 从缓存加载数据地图成功: {cache_path.name}")
            return data
        except Exception as e:
            print(f"[缓存] 加载缓存失败: {e}")
            return None

    def set(self, file_path: str, data_map: dict) -> bool:
        """
        保存数据地图到缓存

        Args:
            file_path: Excel 文件路径
            data_map: DataMap 字典

        Returns:
            是否保存成功
        """
        cache_path = self._get_cache_path(file_path)

        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data_map, f, ensure_ascii=False, indent=2)
            print(f"[缓存] 数据地图已保存: {cache_path.name}")
            return True
        except Exception as e:
            print(f"[缓存] 保存缓存失败: {e}")
            return False

    def clear(self, file_path: str = None):
        """
        清除缓存

        Args:
            file_path: 如果指定，则只清除该文件的缓存；否则清除所有缓存
        """
        if file_path:
            cache_path = self._get_cache_path(file_path)
            if cache_path.exists():
                cache_path.unlink()
                print(f"[缓存] 已清除缓存: {cache_path.name}")
        else:
            # 清除所有缓存
            for cache_file in self.cache_dir.glob("datamap_*.json"):
                cache_file.unlink()
            print(f"[缓存] 已清除所有数据地图缓存")


if __name__ == "__main__":
    # 测试缓存功能
    cache = DataMapCache()

    # 测试数据
    test_data = {
        "data_distribution": {"HSCODE": {"sheet": "Sheet1", "column": "A"}},
        "sheet_structures": {},
        "relationships": [],
        "extraction_strategy": "single_sheet",
        "confidence": 0.9
    }

    # 测试保存
    test_path = "D:\\test\\example.xlsx"
    cache.set(test_path, test_data)

    # 测试读取
    loaded = cache.get(test_path)
    print(f"[测试] 加载结果: {loaded}")

    # 清除测试
    cache.clear(test_path)
