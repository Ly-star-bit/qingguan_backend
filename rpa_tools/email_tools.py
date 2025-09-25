
import configparser
import email
import imaplib
import mimetypes
import os
import re
import smtplib
from loguru import logger
from datetime import datetime, timedelta
from email import encoders
from email.header import decode_header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from tenacity import retry, stop_after_attempt, wait_fixed
def decode_text(data):
    if isinstance(data, str):
        return data
    encodings = ['utf-8', 'gbk', 'latin1']
    for encoding in encodings:
        try:
            decoded_text = data.decode(encoding)
            # print(f"使用编码 {encoding} 解码成功: {decoded_text}")
            return decoded_text
        except UnicodeDecodeError:
            # logger.error(data)
            continue
    raise ValueError("无法使用已知编码解码数据")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5),reraise=True)
def read_email_by_subject(email_user=None, email_pass=None, subject_input=None, seen=False,email_num=1):
    """通过subject查询邮箱并获取附件

    Args:
        email_user (str, optional): 邮箱账户. Defaults to None.
        email_pass (str, optional): 密码或者授权码. Defaults to None.
        subject_input (str, optional): 标题. Defaults to None.
        seen (bool, optional): 是否标记为已读. Defaults to False.
        email_num: 获取几封满足的邮件

    Returns:
        list: 返回匹配的邮箱正文、发件人的邮箱地址和附件
    """
    # 配置解析器实例
    # 获取当前脚本所在目录
    script_dir = os.path.dirname(__file__)
    # 设置配置文件路径
    config_path = os.path.join(script_dir, 'setting.ini')
    
    # 配置解析器实例
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')

    # 如果参数为 None，则从配置文件中获取
    if email_user is None:
        email_user = config.get('IMAPSettings', 'email_user')
    if email_pass is None:
        email_pass = config.get('IMAPSettings', 'email_pass')
    if subject_input is None:
        subject_input = config.get('IMAPSettings', 'subject')

    imap_host = config.get('IMAPSettings', 'imap_server')  # 始终从配置文件获取服务器地址

    # 建立与邮件服务器的安全连接
    mail = imaplib.IMAP4_SSL(imap_host)
    mail.login(email_user, email_pass)
    mail.select("inbox")
    
    subject_encoded = f'{subject_input.strip().lower()}'.encode('gbk')
    try:
        # mail.literal = subject_encoded
        filter_str = f'UNSEEN'
        status, email_ids = mail.search(None, filter_str)

    except imaplib.IMAP4.error as e:
        logger.error(f"error in search email: {str(e)}")
        mail.logout()
        return

    if status != 'OK':
        logger.warning("No unread emails found with the subject!")
        return
    results = []
    for num in list(reversed(email_ids[0].split()))[:50]:
        typ, data = mail.fetch(num, '(RFC822)')
        if not data[0]:
            continue
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject:str = decode_header(msg.get('Subject', ''))[0][0]

                if decode_text(subject_encoded) not in decode_text(subject.strip().lower()):
                    continue
                
                email_body = None
                attachments = []

                if msg.is_multipart():
                    for part in msg.walk():
                        content_disposition = part.get("Content-Disposition", None)
                        if part.get_content_type() == 'text/plain' and content_disposition is None:
                            email_body = part.get_payload(decode=True).decode('gbk', errors='ignore')
                        elif "attachment" in part.get("Content-Disposition", ""):
                            filename = decode_header(part.get_filename())[0][0]
                            if isinstance(filename, bytes):
                                # filename = filename.decode('gbk', errors='ignore')
                                filename = decode_text(filename)
                            attachments.append({
                                'filename': filename,
                                'content': part.get_payload(decode=True)
                            })
                else:
                    email_body = msg.get_payload(decode=True).decode('gbk', errors='ignore')

                from_email = msg.get('From')
                from_email = list(set(re.findall(r"(?:<)?([\w\.-]+@[\w\.-]+\.\w+)(?:>)?", from_email)))[0]
                results.append({
                    'body': email_body,
                    'from': from_email,
                    'attachments': attachments,
                    "subject":decode_text(subject)
                })
                if seen:
                    mail.store(num, 'FLAGS', '\\Seen')

                if len(results) == email_num:
                    #获取指定的几封邮件
                    return results
    mail.close()
    mail.logout()
    return results
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5),reraise=True)
def send_email(smtp_server=None, port=None, username=None, password=None,
               sender_email=None, receiver_email=None, subject=None, body=None, attachments=None, cc=None):
    """发送邮件到目标邮箱

    Args:
        smtp_server (str, optional): smtp服务器地址. Defaults to None.
        port (str, optional): 端口号. Defaults to None.
        username (str, optional): 发送者邮箱. Defaults to None.
        password (str, optional): 发送者密码或者密钥. Defaults to None.
        sender_email (str, optional): 发送人邮箱. Defaults to None.
        receiver_email (str, optional): 接收者邮箱. Defaults to None.
        subject (str, optional): 邮箱主题. Defaults to None.
        body (str, optional): 内容. Defaults to None.
        attachments (_type_, optional): 附件. Defaults to None.
        cc (str, optional): 抄送邮箱. Defaults to None.
    """
        # 配置解析器实例
    # 获取当前脚本所在目录
    script_dir = os.path.dirname(__file__)
    # 设置配置文件路径
    config_path = os.path.join(script_dir, 'setting.ini')
    # 加载配置文件
    config = configparser.ConfigParser()
    config.read(config_path,encoding='utf-8')

    # 如果函数参数为 None，则从配置文件获取值
    smtp_server = smtp_server if smtp_server is not None else config.get('SMTPSettings', 'smtp_server')
    port = port if port is not None else config.getint('SMTPSettings', 'smtp_port')
    username = username if username is not None else config.get('SMTPSettings', 'smtp_user')
    password = password if password is not None else config.get('SMTPSettings', 'smtp_pass')
    sender_email = sender_email if sender_email is not None else config.get('SMTPSettings', 'sender_email')
    receiver_email = receiver_email if receiver_email is not None else config.get('SMTPSettings', 'receiver_email')

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    if cc:
        message['Cc'] = cc
    message['Subject'] = subject if subject is not None else "No Subject"

    message.attach(MIMEText(body, 'plain'))

    if attachments:
        for attachment_path in attachments:
            if os.path.isfile(attachment_path):
                mime_type, _ = mimetypes.guess_type(attachment_path)
                if mime_type is None:
                    mime_type = 'application/octet-stream'  # 如果无法猜测，使用通用类型

                with open(attachment_path, "rb") as attachment:
                    # 根据 MIME 类型创建合适的 MIME part
                    part = MIMEApplication(attachment.read(), _subtype=mime_type.split('/', 1)[1])
                    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
                    message.attach(part)

    with smtplib.SMTP_SSL(smtp_server, port, timeout=30) as server:
         # server.starttls()
        server.login(username, password)
        recipients = [receiver_email]
        if cc:
            recipients.append(cc)
        server.send_message(message, to_addrs=recipients)

       

        logger.info(f"email already send!->{receiver_email}")