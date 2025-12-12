from celery import Celery
from datax_executor import DataXExecutor
import logging
import os
from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND, LOG_LEVEL, LOG_DIR

def setup_logging():
    """
    设置日志配置
    """
    # 确保日志目录存在
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 获取当前模块的logger
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)
    
    # 清除现有的处理器
    logger.handlers.clear()
    
    # 创建文件处理器
    file_handler = logging.FileHandler(
        os.path.join(LOG_DIR, 'celery_app.log'), 
        encoding='utf-8'
    )
    file_handler.setLevel(LOG_LEVEL)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 设置日志
logger = setup_logging()

# 创建Celery应用实例
app = Celery('datax_celery')
app.conf.broker_url = CELERY_BROKER_URL
app.conf.result_backend = CELERY_RESULT_BACKEND

# 创建全局DataX执行器实例
datax_executor = DataXExecutor()


@app.task(bind=True)
def execute_datax_job(self, job_config_path: str, jvm_params: str = None, 
                     job_params: str = None) -> dict:
    """
    Celery任务：执行DataX作业
    
    Args:
        job_config_path: DataX作业配置文件路径
        jvm_params: JVM参数（可选）
        job_params: 作业参数（可选）
        
    Returns:
        执行结果字典
    """
    logger.info(f"开始执行DataX作业: {job_config_path}")
    
    try:
        # 执行DataX作业
        result = datax_executor.execute_job(
            job_config_path=job_config_path,
            jvm_params=jvm_params,
            job_params=job_params
        )
        
        logger.info(f"DataX作业执行完成: {job_config_path}")
        return result
        
    except Exception as e:
        logger.error(f"执行DataX作业时发生异常: {str(e)}")
        # 重新抛出异常以便Celery可以处理重试等机制
        raise self.retry(exc=e, countdown=60, max_retries=3)


@app.task(bind=True)
def validate_datax_job(self, job_config_path: str) -> bool:
    """
    Celery任务：验证DataX作业配置文件
    
    Args:
        job_config_path: DataX作业配置文件路径
        
    Returns:
        配置文件是否有效
    """
    logger.info(f"开始验证DataX作业配置: {job_config_path}")
    
    try:
        # 验证DataX作业配置
        is_valid = datax_executor.validate_job_config(job_config_path)
        
        logger.info(f"DataX作业配置验证完成: {job_config_path}, 结果: {is_valid}")
        return is_valid
        
    except Exception as e:
        logger.error(f"验证DataX作业配置时发生异常: {str(e)}")
        # 重新抛出异常
        raise self.retry(exc=e, countdown=60, max_retries=3)