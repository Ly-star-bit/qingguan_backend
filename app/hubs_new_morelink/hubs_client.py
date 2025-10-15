import base64
import hashlib
import hmac
import json
import os
import pickle
import random
import time
from datetime import datetime, timedelta

import cv2
import httpx
import numpy as np
from captcha_recognizer.slider import Slider
from loguru import logger
from PIL import Image
from httpx import Response
from .new_molink_decrypt import sm3_decrypt


class HubsClient:
    def __init__(
        self, session_file="./session/hubs_session.pkl", retry_counts=5,username=None,password=None,

    ) -> None:
        self.session_file = session_file
        self.retry_counts = retry_counts
        self.httpx_client = None
        self.username = username
        self.password = password
        if self.username and self.password:
             self.httpx_client = self.login_hubs_get_httpx_client()
        else:
            # Check if we have a valid session locally first
            local_client = self.check_and_use_local_session()
            if local_client:
                self.httpx_client = local_client
            else:
                # If no valid session, attempt to log in
                self.httpx_client = self.login_hubs_get_httpx_client()

    def update_token(self, httpx_response_headeres: Response):
        if httpx_response_headeres.get("Access-Token"):
            access_token = httpx_response_headeres.get("Access-Token")
            refresh_token = httpx_response_headeres.get("X-Access-Token")

            self.httpx_client.headers.update(
                {"Authorization": f"Bearer {access_token}"}
            )
            self.httpx_client.headers.update(
                {"X-Authorization": f"Bearer {refresh_token}"}
            )
            self.save_session(access_token, refresh_token)

    def check_and_use_local_session(self):
        """Check if there's a valid session stored locally"""

        client = self.load_session()
        if client:
            # Try a simple request to verify if the access token is still valid
            try:
                # Using a protected endpoint to verify the access token
                resp = client.post(
                    "https://admin.hubs-scs.com/api/v1/pom/sys_user/loginuser",
                    timeout=10,
                    json={},
                )
                if resp.status_code == 200:  # Success with valid token
                    logger.info("本地Hubs client有效")
                    if resp.headers.get("Access-Token"):
                        access_token = resp.headers.get("Access-Token")
                        refresh_token = resp.headers.get("X-Access-Token")
                        client.headers.update(
                            {"Authorization": f"Bearer {access_token}"}
                        )
                        client.headers.update(
                            {"X-Authorization": f"Bearer {refresh_token}"}
                        )
                        self.save_session(access_token, refresh_token)

                    return client
                else:
                    logger.warning(f"本地session验证失败，状态码: {resp.status_code}")
            except Exception as e:
                logger.warning(f"本地session验证失败: {e}")
        return None

    def save_session(self, access_token: str, refresh_token: str):
        """Save the access token to a file"""
        session_data = {"access_token": access_token, "refresh_token": refresh_token}
        os.makedirs(os.path.dirname(self.session_file), exist_ok=True)
        with open(self.session_file, "wb") as f:
            pickle.dump(session_data, f)

    def load_session(self):
        """Load session from file"""
        if os.path.exists(self.session_file):
            with open(self.session_file, "rb") as f:
                session_data = pickle.load(f)
                access_token = session_data.get("access_token")
                refresh_token = session_data.get("refresh_token")
                if access_token:
                    # Create a client with the authorization header
                    headers = {
                        "Accept": "application/json, text/plain, */*",
                        # "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.7",
                        "Connection": "keep-alive",
                        "Content-Type": "application/json",
                        "Host": "admin.hubs-scs.com",
                        "Origin": "https://admin.hubs-scs.com",
                        "Referer": "https://admin.hubs-scs.com/",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                        "Sec-GPC": "1",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                        "X-Requested-With": "XMLHttpRequest",
                        "api-version": "1",
                        "sec-ch-ua": "Chromium;v=140, Not=A?Brand;v=24, Brave;v=140",
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": "Windows",
                    }
                    client = httpx.Client(headers=headers)
                    client.headers.update({"Authorization": f"Bearer {access_token}"})
                    client.headers.update(
                        {"X-Authorization": f"Bearer {refresh_token}"}
                    )
                    return client
        return None

    def get_captcha_images(self):
        """Make a POST request to the captcha API and download the base64-encoded images."""
        picture_dir = "picture"
        os.makedirs(picture_dir, exist_ok=True)

        url = "https://admin.hubs-scs.com/api/v1/login/verification/get_captcha_code"

        payload = {}

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(url, json=payload)
            response.raise_for_status()

            data = response.json()

            if data.get("success") and "data" in data:
                background_b64 = data["data"]["BackgroundImage"]
                slider_b64 = data["data"]["SliderImage"]

                bg_path = os.path.join(picture_dir, "background_image.png")
                slider_path = os.path.join(picture_dir, "slider_image.png")

                self.save_image(background_b64, bg_path)
                self.save_image(slider_b64, slider_path)

                # Combine the images to create a result image for slider detection
                output_path = "./picture/result_image.png"

                # Open images
                slider = Image.open(slider_path)
                bg = Image.open(bg_path)

                # Ensure heights match
                assert slider.height == bg.height, "Heights don't match!"
                assert (
                    slider.width <= bg.width
                ), "Slider width cannot be greater than background!"

                # Create a copy of the background image
                result = bg.copy()

                # Paste the slider at position (0, 0) to cover the left side
                result.paste(slider, (0, 0))

                # Save the result
                result.save(output_path)
                print(f"合成完成：{output_path}")
                return data["data"]["Id"]
            else:
                print(f"API request failed: {data.get('message', 'Unknown error')}")
                return None

        except httpx.RequestError as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    def save_image(self, base64_string, filepath):
        """Decode a base64 string and save it as an image file."""
        if not base64_string:
            print(f"Warning: Image data is empty for {filepath}, skipping.")
            return

        # Remove data URL prefix if present
        if base64_string.startswith("data:image"):
            base64_data = base64_string.split(",", 1)[1]
        else:
            base64_data = base64_string

        try:
            image_data = base64.b64decode(base64_data)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(image_data)
            print(f"Saved image to {filepath}")
        except Exception as e:
            print(f"Error saving image {filepath}: {e}")

    def detect_gap_candidates(self, result_picture_path):
        """Directly get the offset value"""
        slider = Slider()
        offset, offset_confidence = slider.identify_offset(source=result_picture_path)
        box, box_confidence = slider.identify(source=result_picture_path)
        box[0] = box[0] - offset
        return box

    def generate_slider_result(self, original_offset: int) -> dict:
        """Generate slider verification data based on the original offset."""
        if original_offset <= 0:
            original_offset = 100  # fallback

        # Fixed frontend display dimensions
        BG_WIDTH, BG_HEIGHT = 340, 212
        SLIDER_WIDTH, SLIDER_HEIGHT = 68, 212

        # Scale offset to frontend coordinate system
        SCALE = BG_WIDTH / 552  # 340 / 552
        target_x = round(original_offset * SCALE)
        target_x = max(
            10, min(target_x, BG_WIDTH - SLIDER_WIDTH)
        )  # Prevent out of bounds

        # Generate tracks
        total_duration = random.randint(800, 1880)  # ms
        start_delay = random.randint(50, 80)
        point_count = random.randint(22, 32)

        tracks = []
        for i in range(1, point_count + 1):
            ratio = i / point_count
            # ease-out curve: fast first, then slow
            if ratio < 0.8:
                eased = ratio**0.9
            else:
                eased = 1 - (1 - ratio) ** 1.8

            x = min(target_x, max(1, int(target_x * eased)))
            y = -random.randint(0, min(12, int(x / 8) + 2))
            t = start_delay + int(total_duration * ratio)

            if not tracks or x != tracks[-1]["x"] or y != tracks[-1]["y"]:
                tracks.append({"x": x, "y": y, "t": t})

        # Ensure endpoint is accurate
        if tracks and tracks[-1]["x"] != target_x:
            final_t = tracks[-1]["t"] + random.randint(5, 15)
            final_y = -random.randint(8, 12)
            tracks.append({"x": target_x, "y": final_y, "t": final_t})

        # Generate timestamps
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(milliseconds=total_duration + start_delay)

        # Return complete result
        return {
            "backgroundImageWidth": BG_WIDTH,
            "backgroundImageHeight": BG_HEIGHT,
            "sliderImageWidth": SLIDER_WIDTH,
            "sliderImageHeight": SLIDER_HEIGHT,
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "tracks": tracks,
        }

    def perform_login(self, track_id, track_data, headers):
        """Perform the actual login request with captcha data"""
        url = "https://admin.hubs-scs.com/api/v1/login/somauth/login"

        data = {
            "id": track_id,
            "loginpwd": sm3_decrypt("admin123") if not self.password else sm3_decrypt(self.password),
            "loginuser": "admin" if not self.username else self.username,
            "track": track_data,
        }

        try:
            with httpx.Client(headers=headers) as client:
                # First, login to get initial token
                response = client.post(url, json=data)
                # response.raise_for_status()  # Check for HTTP errors
                login_response = response.json()
                if login_response['code'] == 400 :
                    return login_response
                access_token = login_response["data"]["list"][0]["accessToken"]
                headers["Authorization"] = f"Bearer {access_token}"
                if login_response.get("success"):
                    # After successful login, call loginconfig to get the real tokens
                    config_url = (
                        "https://admin.hubs-scs.com/api/v1/login/somauth/loginconfig"
                    )
                    config_data = {"tenantid": "10849", "tid": 10849}

                    config_response = client.post(
                        config_url, json=config_data, headers=headers
                    )
                    config_response.raise_for_status()

                    # Get the real tokens from response headers
                    access_token = config_response.headers.get(
                        "Access-Token"
                    ) or config_response.headers.get("access-token")
                    x_access_token = config_response.headers.get(
                        "X-Access-Token"
                    ) or config_response.headers.get("x-access-token")

                    # Update the login response with the real tokens
                    if access_token or x_access_token:
                        login_response["data"]["list"][0]["accessToken"] = (
                            access_token
                            or login_response["data"]["list"][0].get("accessToken")
                        )
                        login_response["real_access_token"] = access_token
                        login_response["x_access_token"] = x_access_token

                    return login_response
                else:
                    # If initial login failed, return the original response
                    return login_response

        except httpx.RequestError as e:
            return {"error": f"Request error: {str(e)}"}
        except json.JSONDecodeError as e:
            return {
                "error": f"JSON decode error: {str(e)}",
                "content": response.text if "response" in locals() else "",
            }
        except Exception as e:
            return {"error": f"Unknown error: {str(e)}"}

    def login_hubs_get_httpx_client(self):
        """Attempt to log in to Hubs and return an httpx client"""
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.7",
            "Content-Type": "application/json",
            "Origin": "https://admin.hubs-scs.com",
            "Referer": "https://admin.hubs-scs.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "api-version": "1",
        }

        for count in range(1, self.retry_counts + 1):
            logger.info(f"开始第{count}次尝试Hubs登陆")

            # Get captcha images
            track_id = self.get_captcha_images()
            if not track_id:
                logger.error(f"获取验证码失败，第{count}次尝试")
                continue

            # Detect gap and generate slider result
            output_path = r"picture\result_image.png"
            try:
                box = self.detect_gap_candidates(output_path)
                track_data = self.generate_slider_result(original_offset=box[0])
            except Exception as e:
                logger.error(f"处理验证码图片失败: {e}，第{count}次尝试")
                continue

            # Perform the login request
            login_result = self.perform_login(track_id, track_data, headers)

            if login_result and login_result.get("success"):
                # Extract the access token from the login response
                try:
                    # Use the real access token if available
                    real_access_token = login_result.get("real_access_token")
                    if real_access_token:
                        access_token = real_access_token
                        refresh_token = login_result.get("x_access_token")

                    logger.info(f"尝试{count}次登陆成功")
                    # Create client with both authorization header and other headers
                    client = httpx.Client(headers=headers)
                    client.headers.update(
                        {
                            "Authorization": f"Bearer {access_token}",
                            "X-Authorization": f"Bearer {refresh_token}",
                        }
                    )  # Add auth header

                    # Save session for future use
                    if self.username :
                        return  {
                            "Authorization": f"Bearer {access_token}",
                            "X-Authorization": f"Bearer {refresh_token}",
                        }
                    self.save_session(access_token, refresh_token)
                    return client
                except (KeyError, IndexError) as e:
                    logger.error(f"无法从登录响应中提取访问令牌: {e}")
                    continue
            else:
                logger.info(login_result)
                if login_result['code'] == 400:
                    logger.error("用户或密码错误")
                    return login_result

                logger.error(f"{count}次登陆失败: {login_result}")

                time.sleep(random.uniform(1, 3))  # Wait before retrying

        logger.error("Hubs登陆失败，已尝试最大次数")
        return None

    def get_customer_channels(
        self, keyword="", pageindex=1, pagesize=30, channel_type=2
    ):
        """
        Get customer channels from the API
        :param keyword: Search keyword (default: "")
        :param pageindex: Page index (default: 1)
        :param pagesize: Page size (default: 30)
        :param channel_type: Channel type (default: 2)
        :return: List of channels or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/sys_channel/getcomponentlist"

        payload = {
            "keyword": keyword,
            "pageindex": pageindex,
            "pagesize": pagesize,
            "channel_type": channel_type,
        }

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)
            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get customer channels: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting customer channels: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting customer channels: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting customer channels: {e}")
            return None

    def get_base_countries(self, keyword="", pagesize=499):
        """
        Get base countries from the API
        :param pagesize: Page size (default: 499)
        :return: List of countries or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/base_country/getcomponentlist"

        payload = {"pageSize": pagesize, "open_search": True, "keyword": keyword}

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get base countries: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting base countries: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting base countries: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting base countries: {e}")
            return None

    def get_import_warehouses(self, keyword="", pagesize=499):
        """
        Get warehouses from the API
        :param pagesize: Page size (default: 499)
        :return: List of warehouses or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/sys_warehouse/getcomponentlist"

        payload = {"pageSize": pagesize, "keyword": keyword}

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)
            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def get_base_fba(self, keyword="", pagesize=499):
        """
        Get base fba from the API
        :param pagesize: Page size (default: 499)
        :return: List of base fba or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/base_fbx/getcomponentlist"

        payload = {"pageindex": 1, "pageSize": pagesize, "keyword": keyword}

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def get_crm_supplier(self, keyword="", pagesize=499):
        """
        Getcrm_supplier from the API
        :param pagesize: Page size (default: 499)
        :return: List of crm_supplier or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/crm_supplier/getcomponentlist"

        payload = {
            "pageindex": 1,
            "pageSize": pagesize,
            "keyword": keyword,
            "isgroup": True,
        }

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def get_sys_customer(self, keyword="", pagesize=499):
        """
        Get sys_customer from the API
        :param pagesize: Page size (default: 499)
        :return: List of sys_customer or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/pom/sys_customer/getcomponentlist"

        payload = {
            "pageindex": 1,
            "pageSize": pagesize,
            "keyword": keyword,
        }

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def get_base_fbx_addr(self, keyword="", pagesize=499):
        """
        Get base_fbx_addr from the API
        :param pagesize: Page size (default: 499)
        :return: List of base_fbx_addr or None if request fails
        """
        url = (
            "https://admin.hubs-scs.com/api/v1/pom/base_fbx_addr/getcomponentlist_byall"
        )

        payload = {
            "pageindex": 1,
            "pageSize": pagesize,
            "keyword": keyword,
        }

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data and "list" in data["data"]:
                return data["data"]["list"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def get_bigwaybill_order(self, sono):
        """
        Get 大货运单 from the API
        :param pagesize: Page size (default: 499)
        :return: List of 大货运单 or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/oms/oms_bigwaybill_order/getmodel"

        payload = {"ids": sono}

        try:
            response = self.httpx_client.post(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data:
                return data["data"]
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None

    def post_bigwaybill_order(self, payload):
        """
        新建 大货运单 from the API
        :param pagesize: Page size (default: 499)
        :return: List of 大货运单 or None if request fails
        """
        url = "https://admin.hubs-scs.com/api/v1/oms/oms_bigwaybill_order/newadd"

        try:
            response = self.httpx_client.put(url, json=payload)
            # response.raise_for_status()

            data = response.json()
            self.update_token(response.headers)

            if data.get("success") and "data" in data:
                return data
            else:
                logger.error(
                    f"Failed to get warehouses: {data.get('message', 'Unknown error')}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Request error while getting warehouses: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error while getting warehouses: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error while getting warehouses: {e}")
            return None


def main():
    # Create a HubsClient instance which will handle login automatically
    client = HubsClient(retry_counts=5)

    if client.httpx_client:
        print("Successfully logged in and obtained httpx.Client")
        dahuoyundan_data = client.get_bigwaybill_order(sono=["0829231759210286304"])
        customers_channels = client.get_customer_channels("空运经济")
        base_countries = client.get_base_countries("中国")
        import_warehouse = client.get_import_warehouses("上海仓")
        d_code_data = client.get_base_fbx_addr("ABE2")
        customer = client.get_sys_customer("SZNE-深圳纽尔")
        print(123)
        # Example of using the new method:
        # channels = client.get_customer_channels()
        # if channels:
        #     print(f"Retrieved {len(channels)} channels")
        #     for channel in channels:
        #         print(f"Channel: {channel['channel_name']} - {channel['channel_code']}")
    else:
        print("Failed to log in and obtain httpx.Client")


if __name__ == "__main__":
    main()
