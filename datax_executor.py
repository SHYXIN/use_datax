import subprocess
import json
import os
import logging
from typing import Dict, Any, Optional
from config import DATAX_PY_PATH, LOG_LEVEL, LOG_DIR

# 配置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'datax_executor.log'), encoding='utf-8'),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)
logger = logging.getLogger(__name__)


class DataXExecutor:
    """
    DataX执行器类，用于封装DataX工具的调用和执行
    """

    def __init__(self):
        """
        初始化DataX执行器
        """
        if not os.path.exists(DATAX_PY_PATH):
            raise FileNotFoundError(f"DataX执行脚本不存在: {DATAX_PY_PATH}")
        
        self.datax_py_path = DATAX_PY_PATH

    def execute_job(self, job_config_path: str, jvm_params: Optional[str] = None, 
                   job_params: Optional[str] = None) -> Dict[str, Any]:
        """
        执行DataX作业
        
        Args:
            job_config_path: DataX作业配置文件路径
            jvm_params: JVM参数（可选）
            job_params: 作业参数（可选）
            
        Returns:
            执行结果字典，包含状态码、输出等信息
        """
        if not os.path.exists(job_config_path):
            raise FileNotFoundError(f"作业配置文件不存在: {job_config_path}")

        # 构建命令
        cmd = ['python', self.datax_py_path]
        
        # 添加JVM参数
        if jvm_params:
            cmd.extend(['-j', jvm_params])
            
        # 添加作业参数
        if job_params:
            cmd.extend(['-p', job_params])
            
        # 添加作业配置文件路径
        cmd.append(job_config_path)
        
        logger.info(f"执行DataX作业: {' '.join(cmd)}")
        
        try:
            # 执行命令
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate()
            
            result = {
                'return_code': process.returncode,
                'stdout': stdout,
                'stderr': stderr,
                'success': process.returncode == 0
            }
            
            if process.returncode == 0:
                logger.info("DataX作业执行成功")
            else:
                logger.error(f"DataX作业执行失败: {stderr}")
                
            return result
            
        except Exception as e:
            logger.error(f"执行DataX作业时发生异常: {str(e)}")
            return {
                'return_code': -1,
                'stdout': '',
                'stderr': str(e),
                'success': False
            }

    def validate_job_config(self, job_config_path: str) -> bool:
        """
        验证DataX作业配置文件是否有效
        
        Args:
            job_config_path: DataX作业配置文件路径
            
        Returns:
            配置文件是否有效
        """
        try:
            with open(job_config_path, 'r', encoding='utf-8') as f:
                job_config = json.load(f)
                
            # 检查必要的字段
            if 'job' not in job_config:
                logger.error("作业配置缺少'job'字段")
                return False
                
            if 'content' not in job_config['job']:
                logger.error("作业配置缺少'job.content'字段")
                return False
                
            return True
        except Exception as e:
            logger.error(f"验证作业配置文件时发生异常: {str(e)}")
            return False