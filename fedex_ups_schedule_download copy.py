import datetime
from io import BytesIO
import time
import httpx
from pathlib import Path
from loguru import logger
from playwright.sync_api import sync_playwright
import PyPDF2
import pandas as pd
import hashlib



def get_file_md5(file_path):
    """
    计算文件的MD5值
    """
    md5_hash = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except FileNotFoundError:
        return None

def get_bytes_md5(content):
    """
    计算字节流的MD5值
    """
    md5_hash = hashlib.md5()
    md5_hash.update(content)
    return md5_hash.hexdigest()

def download_fedex_das_pdf():
    """
    下载FedEx DAS PDF文件
    """
        # 获取当年的年份字符串
    current_year = str(datetime.now().year)
    print(current_year)  # 输出: 2025
    url = f"https://www.fedex.com/content/dam/fedex/us-united-states/services/DAS_Contiguous_Extended_Remote_Alaska_Hawaii_{current_year}.xlsx"



    save_path = Path(rf"file\\remoteaddresscheck\\DAS_Contiguous_Extended_Remote_Alaska_Hawaii_{current_year}.xlsx")
    
    try:
        response = httpx.get(url,timeout=60)
        if response.status_code == 200:
            excel_content = response.content
            new_md5 = get_bytes_md5(excel_content)
            logger.info(f"新文件MD5: {new_md5}")
            
            # 检查是否存在旧版本文件
            if save_path.exists():
                old_md5 = get_file_md5(save_path)
                logger.info(f"旧文件MD5: {old_md5}")
                
                if old_md5 == new_md5:
                    logger.info("MD5值一致，文件未更新，跳过下载")
                    return None
            
            # MD5不一致或文件不存在，进行保存
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(excel_content)
            logger.info(f"成功下载UPS DAS文件到: {save_path}")
            return str(save_path)
        else:
            logger.error(f"下载UPS DAS文件失败,状态码: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"下载UPS DAS文件时发生错误: {e}")
        return None

def download_ups_das_pdf():

    url = "https://delivery-p55671-e392469.adobeaemcloud.com/adobe/assets/urn:aaid:aem:5abedc88-3398-4cec-98b7-08e0bb271544/original/as/area-surcharge-zips-us-en.xlsx"
    save_path = Path(r"file\\remoteaddresscheck\\area-surcharge-zips-us-en.xlsx")
    
    try:
        response = httpx.get(url,timeout=60)
        if response.status_code == 200:
            excel_content = response.content
            new_md5 = get_bytes_md5(excel_content)
            logger.info(f"新文件MD5: {new_md5}")
            
            # 检查是否存在旧版本文件
            if save_path.exists():
                old_md5 = get_file_md5(save_path)
                logger.info(f"旧文件MD5: {old_md5}")
                
                if old_md5 == new_md5:
                    logger.info("MD5值一致，文件未更新，跳过下载")
                    return None
            
            # MD5不一致或文件不存在，进行保存
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(excel_content)
            logger.info(f"成功下载UPS DAS文件到: {save_path}")
            return str(save_path)
        else:
            logger.error(f"下载UPS DAS文件失败,状态码: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"下载UPS DAS文件时发生错误: {e}")
        return None

if __name__ == "__main__":
    # schedule.every().month.at("00:00").do(download_fedex_das_pdf)
    # schedule.every().month.at("00:00").do(download_ups_das_pdf)
    
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)
    download_fedex_das_pdf()
    download_ups_das_pdf()
