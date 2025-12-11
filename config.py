import os

# DataX相关配置
DATAX_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datax')
DATAX_PY_PATH = os.path.join(DATAX_HOME, 'bin', 'datax.py')

# Celery配置
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# 日志配置
LOG_LEVEL = 'INFO'
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

# 确保日志目录存在
os.makedirs(LOG_DIR, exist_ok=True)