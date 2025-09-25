from io import BytesIO
import time
import httpx
from pathlib import Path
from loguru import logger
from playwright.sync_api import sync_playwright
import schedule
import PyPDF2
import pandas as pd
import setting
def download_fedex_das_pdf():
    """
    下载FedEx DAS PDF文件
    """
    url = "https://www.fedex.com/content/dam/fedex/us-united-states/services/DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf"
    save_path = Path(r"file\remoteaddresscheck\DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf")
    
    try:
        with sync_playwright() as p:
            browser = p.firefox.launch()
            context = browser.new_context()
            api_content = context.request
            # 下载PDF文件
            response = api_content.get(url)
            if response.ok:
                pdf_content = response.body()
                #读取pdf的第一页，获取更新时间
                pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))

                first_page = pdf_reader.pages[0]
                update_time = first_page.extract_text().split("Effective")[1].split(".")[0].split("\n")[0].strip()
                logger.info(f"更新时间: {update_time}")
                if update_time != setting.fedex_update:
                    save_path.write_bytes(pdf_content)
                    logger.info(f"成功下载FedEx DAS PDF文件到: {save_path}")
                    setting.fedex_update = update_time    
                    browser.close()
                    return str(save_path)
            else:
                logger.error(f"下载FedEx DAS PDF失败,状态码: {response.status}")
                browser.close()
                return None
                
    except Exception as e:
        logger.error(f"下载FedEx DAS PDF时发生错误: {e}")
        return None

def download_ups_das_pdf():
    url = "https://delivery-p55671-e392469.adobeaemcloud.com/adobe/assets/urn:aaid:aem:5abedc88-3398-4cec-98b7-08e0bb271544/original/as/area-surcharge-zips-us-en.xlsx"
    save_path = Path(r"file\remoteaddresscheck\area-surcharge-zips-us-en.xlsx")
    try:
        response = httpx.get(url)
        if response.status_code == 200:
            excel_content = response.content
            #pandas 读取excel，默认sheet的cell b8
            df = pd.read_excel(BytesIO(excel_content))
            
            update_time = df.iloc[6, 1]
            logger.info(f"更新时间: {update_time}")
            if update_time != setting.ups_update:
                save_path.write_bytes(excel_content)    
                logger.info(f"成功下载UPS DAS PDF文件到: {save_path}")
                setting.ups_update = update_time    
                return str(save_path)
        else:
            logger.error(f"下载UPS DAS PDF失败,状态码: {response.status}")
            return None
    except Exception as e:  
        logger.error(f"下载UPS DAS PDF时发生错误: {e}")
        return None

if __name__ == "__main__":
    # schedule.every().month.at("00:00").do(download_fedex_das_pdf)
    # schedule.every().month.at("00:00").do(download_ups_das_pdf)
    
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)
    download_fedex_das_pdf()
    download_ups_das_pdf()
