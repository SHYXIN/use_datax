from celery_app import execute_datax_job, validate_datax_job
import logging
import os
from typing import Optional
from config import LOG_LEVEL, LOG_DIR

# 配置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'tasks_scheduler.log'), encoding='utf-8'),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)
logger = logging.getLogger(__name__)


class DataXTaskScheduler:
    """
    DataX任务调度器，提供便捷的方法来调度和执行DataX作业
    """

    def __init__(self):
        """
        初始化任务调度器
        """
        pass

    def schedule_job_execution(self, job_config_path: str, jvm_params: Optional[str] = None,
                              job_params: Optional[str] = None, queue: str = 'celery') -> str:
        """
        调度执行DataX作业
        
        Args:
            job_config_path: DataX作业配置文件路径
            jvm_params: JVM参数（可选）
            job_params: 作业参数（可选）
            queue: 任务队列名称，默认为'datax'
            
        Returns:
            任务ID
        """
        logger.info(f"调度执行DataX作业: {job_config_path}")
        
        # 异步执行任务
        task = execute_datax_job.apply_async(
            args=[job_config_path],
            kwargs={
                'jvm_params': jvm_params,
                'job_params': job_params
            },
            queue=queue
        )
        
        logger.info(f"已提交作业执行任务，任务ID: {task.id}")
        return task.id

    def schedule_job_validation(self, job_config_path: str, queue: str = 'celery') -> str:
        """
        调度验证DataX作业配置
        
        Args:
            job_config_path: DataX作业配置文件路径
            queue: 任务队列名称，默认为'datax'
            
        Returns:
            任务ID
        """
        logger.info(f"调度验证DataX作业配置: {job_config_path}")
        
        # 异步执行任务
        task = validate_datax_job.apply_async(
            args=[job_config_path],
            queue=queue
        )
        
        logger.info(f"已提交作业验证任务，任务ID: {task.id}")
        return task.id

    def get_task_result(self, task_id: str):
        """
        获取任务执行结果
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务执行结果
        """
        logger.info(f"获取任务执行结果，任务ID: {task_id}")
        
        # 获取任务结果
        result = execute_datax_job.AsyncResult(task_id)
        return result

    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务执行
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功取消
        """
        logger.info(f"取消任务执行，任务ID: {task_id}")
        
        # 取消任务
        result = execute_datax_job.control.revoke(task_id, terminate=True)
        return result is not None