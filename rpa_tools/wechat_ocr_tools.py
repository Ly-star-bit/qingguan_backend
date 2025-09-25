import os
import winreg
import re

def find_wechat_versioned_path(base_path):
    # 正则表达式匹配类似版本号的文件夹
    pattern = re.compile(r'\[\d+\.\d+\.\d+\.\d+\]')
    for entry in os.listdir(base_path):
        if pattern.match(entry):
            return os.path.join(base_path, entry)
    return None

def find_wechat_path():
    common_paths = [
        r"C:\Program Files\Tencent\WeChat",
        r"C:\Program Files (x86)\Tencent\WeChat",
        os.path.join(os.getenv('PROGRAMFILES'), 'Tencent', 'WeChat'),
        os.path.join(os.getenv('PROGRAMFILES(X86)'), 'Tencent', 'WeChat')
    ]

    for path in common_paths:
        if os.path.isdir(path):
            # 在找到的路径中搜索包含版本号的子目录
            versioned_path = find_wechat_versioned_path(path)
            if versioned_path:
                return versioned_path

    # 使用Windows注册表查找WeChat路径
    registry_paths = [
        r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\WeChat.exe",
        r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\App Paths\WeChat.exe"
    ]
    for reg_path in registry_paths:
        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
            path, _ = winreg.QueryValueEx(key, "")
            if path:
                base_path = os.path.dirname(path)
                versioned_path = find_wechat_versioned_path(base_path)
                if versioned_path:
                    return versioned_path
        except FileNotFoundError:
            pass
        except OSError:
            pass

    return None



def find_wechat_ocr_executable(base_path):
    # 遍历给定目录下的所有子目录
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # 检查是否是WeChatOCR.exe文件
            if file == "WeChatOCR.exe":
                return os.path.join(root, file)
    return None

def find_wechat_ocr_path():
    # 获取当前用户的AppData\Roaming目录
    appdata_roaming = os.getenv('APPDATA')  # 指向C:\Users\{username}\AppData\Roaming
    wechat_ocr_base_path = os.path.join(appdata_roaming, "Tencent", "WeChat", "XPlugin", "Plugins", "WeChatOCR")

    # 检查WeChatOCR基础目录是否存在
    if os.path.exists(wechat_ocr_base_path):
        # 在WeChatOCR目录下查找WeChatOCR.exe
        wechat_ocr_executable_path = find_wechat_ocr_executable(wechat_ocr_base_path)
        if wechat_ocr_executable_path:
            return wechat_ocr_executable_path
    return None