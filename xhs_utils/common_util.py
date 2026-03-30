import os
from loguru import logger
from dotenv import load_dotenv

def load_env():
    load_dotenv()
    cookies_str = os.getenv('COOKIES')
    return cookies_str

def load_mysql_config():
    mysql_host = os.getenv('MYSQL_HOST', '')
    mysql_user = os.getenv('MYSQL_USER', '')
    mysql_password = os.getenv('MYSQL_PASSWORD', '')
    mysql_database = os.getenv('MYSQL_DATABASE', '')
    mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
    if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
        return None

    if mysql_database == 'MYSQL_DATABASE':
        logger.warning('检测到 MYSQL_DATABASE 使用了占位符字符串，请改成真实库名，例如 xiaohongshudata')

    return {
        'host': mysql_host,
        'port': mysql_port,
        'user': mysql_user,
        'password': mysql_password,
        'database': mysql_database,
    }


def init():
    media_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datas/media_datas'))
    excel_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datas/excel_datas'))
    for base_path in [media_base_path, excel_base_path]:
        if not os.path.exists(base_path):
            os.makedirs(base_path)
            logger.info(f'创建目录 {base_path}')
    cookies_str = load_env()
    base_path = {
        'media': media_base_path,
        'excel': excel_base_path,
    }
    return cookies_str, base_path
