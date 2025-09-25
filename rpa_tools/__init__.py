import os
from pathlib import Path
import site
import sys
import httpx
from playwright.sync_api import BrowserContext
from .chrome_tools import ChromeLauncher,WindowSetter
from .email_tools import send_email,read_email_by_subject
from .excel_tools import get_excel_application,write_data_to_excel
from .wechat_ocr_tools import find_wechat_ocr_path,find_wechat_path



def find_playwright_node_path():
    # 定位到 Python 环境中的 site-packages 目录
    site_packages_path = Path(site.getsitepackages()[-1])
    # 检查系统平台
    if sys.platform.startswith('win'):
        # Windows 系统的路径处理
        node_path = site_packages_path / 'playwright' / 'driver' / 'node.exe'
    else:
        # macOS 和 Linux 系统的路径处理
        node_path = site_packages_path / 'playwright' / 'driver' / 'bin' / 'node'

    # 返回找到的 Node 路径，如果存在的话
    if node_path.exists():
        # os.environ["EXECJS_RUNTIME"] = "Node"
        # os.environ["NODE_PATH"] = str(node_path)
        return str(node_path)
    else:
        return "Node.js path not found in the expected directory."