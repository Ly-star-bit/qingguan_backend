"""
# @ Author: luoyu
# @ Create Time: 2024-04-15 13:39:00
# @ Modified by: luoyu
# @ Modified time: 2024-04-16 11:14:33
# @ Description: 端口启动连接chrome(chromium)
"""

import subprocess
import socket
import time
import configparser
import os
import platform
from loguru import logger

# from tms_import import login_import
import socket
from time import sleep
from playwright.sync_api import  CDPSession

# Only import winreg on Windows
if platform.system() == "Windows":
    import winreg


class ChromeLauncher:
    def __init__(self, config_path=None, multi_chrome=False):
        """初始化Chrome

        Args:
            config_path (str, optional): 配置文件. Defaults to 'setting.ini'.
            multi_chrome (bool, optional): 是否启用多个浏览器进程. Defaults to False.
        """
        if not config_path:
            # 获取当前脚本所在目录
            script_dir = os.path.dirname(__file__)
            # 设置配置文件路径
            config_path = os.path.join(script_dir, 'setting.ini')
        # 加载配置文件
        config = configparser.ConfigParser()
        config.read(config_path,encoding='utf-8')

        if (
            config.has_option("ChromeSettings", "chrome_path")
            and config.get("ChromeSettings", "chrome_path").strip()
        ):
            self.chrome_path = config.get("ChromeSettings", "chrome_path").strip()
        else:
            self.chrome_path = self.find_chrome_path()
        logger.info(self.chrome_path)
        self.multi_chrome = multi_chrome
        self.user_data_dir = config.get(
            "ChromeSettings",
            "user_data_dir",
            fallback=os.path.join(
                os.getenv("LOCALAPPDATA"), "Google", "Chrome", "User Data"
            ),
        )
        self.debug_port = int(
            config.get("ChromeSettings", "debug_port", fallback="9222")
        )
        self.process = None

    def find_chrome_path(self):
        system = platform.system()
        
        if system == "Windows":
            common_paths = [
                "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
                "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
                os.path.join(
                    os.getenv("LOCALAPPDATA"),
                    "Google",
                    "Chrome",
                    "Application",
                    "chrome.exe",
                ),
            ]
        elif system == "Linux":
            common_paths = [
                "/usr/bin/google-chrome",
                "/usr/bin/chromium-browser",
                "/usr/bin/chromium",
                "/usr/bin/google-chrome-stable",
                "/usr/bin/google-chrome-beta",
                "/usr/bin/google-chrome-unstable",
                os.path.join(
                    os.getenv("HOME"),
                    ".google-cloud-sdk",
                    "bin",
                    "google-cloud-sdk",
                    "google-cloud-sdk",
                    "bin",
                    "chrome"
                )
            ]
        else:
            # For macOS or other systems
            common_paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/usr/bin/google-chrome"
            ]

        for path in common_paths:
            if os.path.isfile(path):
                return path

        # Windows registry lookup (Windows only)
        if system == "Windows":
            try:
                key = winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe",
                )
                path, _ = winreg.QueryValueEx(key, "")
                return path
            except FileNotFoundError:
                pass

            try:
                key = winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe",
                )
                path, _ = winreg.QueryValueEx(key, "")
                return path
            except FileNotFoundError:
                pass

        return None

    def is_port_in_use(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            state = s.connect_ex(("localhost", port))
            if state == 0:
                return True
            return False

    def launch_chrome(self):
        port_to_try = self.debug_port
        while True:
            if self.is_port_in_use(port_to_try):
                if self.multi_chrome:
                    logger.info(
                        f"Port {port_to_try} is already in use. Trying next port."
                    )
                    port_to_try += 1
                else:
                    logger.info(
                        f"Port {port_to_try} is already in use. Chrome won't be launched on this port."
                    )
                    return False
            else:
                command = [
                    self.chrome_path,
                    f"--remote-debugging-port={port_to_try}",
                    f"--user-data-dir={self.user_data_dir}\{port_to_try}",
                    "--no-default-browser-check",
                    "--disable-suggestions-ui",
                    "--no-first-run",
                    "--disable-infobars",
                    "--disable-popup-blocking",
                    "--hide-crash-restore-bubble",
                    "--disable-features=PrivacySandboxSettings4",
                ]
                self.process = subprocess.Popen(command)
                logger.info(f"Launched Chrome on port {port_to_try}.")
                self.debug_port = port_to_try
                time.sleep(5)

                return True

    def close_chrome(self):
        if self.process is not None:
            self.process.terminate()
            logger.info("Chrome process has been terminated.")

    def __enter__(self):
        self.launch_chrome()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_chrome()
        if exc_type is not None:
            logger.error(f"Exception type: {exc_type}")
            logger.error(f"Exception value: {exc_value}")
            logger.error(f"Traceback: {traceback}")
class WindowSetter:
    """用于设置窗口大小的类"""

    def __init__(self, owner: CDPSession):
        self._owner = owner
        self._window_id = self._get_info()["windowId"]

    def max(self):
        """窗口最大化"""
        if self._get_info()["bounds"]["windowState"] in ("fullscreen", "minimized"):
            self._perform({"windowState": "normal"})
        self._perform({"windowState": "maximized"})

    def mini(self):
        """窗口最小化"""
        if self._get_info()["bounds"]["windowState"] == "fullscreen":
            self._perform({"windowState": "normal"})
        self._perform({"windowState": "minimized"})

    def full(self):
        """设置窗口为全屏"""
        if self._get_info()["bounds"]["windowState"] == "minimized":
            self._perform({"windowState": "normal"})
        self._perform({"windowState": "fullscreen"})

    def normal(self):
        """设置窗口为常规模式"""
        self._perform({"windowState": "normal"})

    def size(self, width=None, height=None):
        """设置窗口大小"""
        self.normal()
        info = self._get_info()["bounds"]
        new_width = width or info["width"]
        new_height = height or info["height"]
        self._perform({"width": new_width, "height": new_height})

    def location(self, x=None, y=None):
        """设置窗口在屏幕中的位置"""
        self.normal()
        info = self._get_info()["bounds"]
        new_x = x if x is not None else info["left"]
        new_y = y if y is not None else info["top"]
        self._perform({"left": new_x, "top": new_y})

    def _get_info(self):
        """获取窗口位置及大小信息"""
        for _ in range(50):
            try:
                return self._owner.send("Browser.getWindowForTarget")
            except Exception as e:
                sleep(0.1)
                print(f"Error getting window info: {e}")
        raise RuntimeError("Failed to get window info after multiple attempts.")

    def _perform(self, bounds):
        """执行改变窗口大小操作"""
        try:
            self._owner.send(
                "Browser.setWindowBounds",
                {"windowId": self._window_id, "bounds": bounds},
            )
        except Exception as e:
            print(e)
            raise RuntimeError(
                "Failed to change window state. Ensure proper window state before resizing."
            )
