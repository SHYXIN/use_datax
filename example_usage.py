"""
DataX-Celery项目使用示例
"""

from tasks_scheduler import DataXTaskScheduler
import os
import time

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATAX_JOB_PATH = os.path.join(PROJECT_ROOT, 'datax', 'job', 'job.json')
SAMPLE_MYSQL_JOB_PATH = os.path.join(PROJECT_ROOT, 'datax', 'job', 'datax_14_t_day_to_12_t_day-20251202103452.json')

# 检查必要的文件是否存在
if not os.path.exists(DATAX_JOB_PATH):
    print(f"警告: 默认作业配置文件不存在: {DATAX_JOB_PATH}")
    
if not os.path.exists(SAMPLE_MYSQL_JOB_PATH):
    print(f"警告: MySQL示例作业配置文件不存在: {SAMPLE_MYSQL_JOB_PATH}")


def example_basic_usage():
    """基本使用示例"""
    print("=== DataX-Celery 基本使用示例 ===")
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    # 调度执行DataX作业
    task_id = scheduler.schedule_job_execution(
        job_config_path=DATAX_JOB_PATH,
        jvm_params="-Xms512m -Xmx1g",
        job_params="-Dparam1=value1 -Dparam2=value2"
    )
    
    print(f"已提交作业执行任务，任务ID: {task_id}")
    
    # 等待一段时间让任务执行完成
    time.sleep(5)
    
    # 获取任务执行结果
    result = scheduler.get_task_result(task_id)
    print(f"任务状态: {result.state}")
    if result.ready():
        print(f"任务执行结果: {result.result}")
    
    # 检查执行是否成功
    if result.successful():
        execution_result = result.result
        if execution_result.get('success', False):
            print("DataX作业执行成功！")
        else:
            print(f"DataX作业执行失败: {execution_result.get('stderr', '未知错误')}")


def example_job_validation():
    """作业配置验证示例"""
    print("\n=== DataX作业配置验证示例 ===")
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    # 调度验证DataX作业配置
    task_id = scheduler.schedule_job_validation(DATAX_JOB_PATH)
    
    print(f"已提交作业验证任务，任务ID: {task_id}")
    
    # 等待一段时间让任务执行完成
    time.sleep(3)
    
    # 获取任务执行结果
    result = scheduler.get_task_result(task_id)
    print(f"任务状态: {result.state}")
    if result.ready():
        is_valid = result.result
        print(f"作业配置验证结果: {'有效' if is_valid else '无效'}")
        
        # 如果配置无效，打印错误信息
        if not is_valid:
            print("请检查作业配置文件是否符合DataX规范")


def example_custom_queue():
    """自定义队列示例"""
    print("\n=== 自定义队列示例 ===")
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    # 使用自定义队列调度任务
    task_id = scheduler.schedule_job_execution(
        job_config_path=DATAX_JOB_PATH,
        queue='high_priority'  # 使用高优先级队列
    )
    
    print(f"已提交到高优先级队列的任务ID: {task_id}")


def example_mysql_job_usage():
    """MySQL作业示例"""
    print("\n=== MySQL作业使用示例 ===")
    
    # 检查MySQL作业配置文件是否存在
    if not os.path.exists(SAMPLE_MYSQL_JOB_PATH):
        print(f"警告: MySQL作业配置文件不存在: {SAMPLE_MYSQL_JOB_PATH}")
        return
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    # 调度执行MySQL DataX作业
    task_id = scheduler.schedule_job_execution(
        job_config_path=SAMPLE_MYSQL_JOB_PATH,
        jvm_params="-Xms512m -Xmx1g"
    )
    
    print(f"已提交MySQL作业执行任务，任务ID: {task_id}")
    
    # 等待一段时间让任务执行完成
    time.sleep(5)
    
    # 获取任务执行结果
    result = scheduler.get_task_result(task_id)
    print(f"任务状态: {result.state}")
    if result.ready():
        print(f"任务执行结果: {result.result}")
    
    # 检查执行是否成功
    if result.successful():
        execution_result = result.result
        if execution_result.get('success', False):
            print("MySQL DataX作业执行成功！")
        else:
            print(f"MySQL DataX作业执行失败: {execution_result.get('stderr', '未知错误')}")


if __name__ == "__main__":
    print("欢迎使用 DataX-Celery 示例程序！")
    print("=" * 50)
    
    # 运行示例
    example_basic_usage()
    example_job_validation()
    example_custom_queue()
    example_mysql_job_usage()  # 添加这一行来调用新的示例函数
    
    print("\n" + "=" * 50)
    print("=== 使用说明 ===")
    print("1. 确保已安装并启动Redis服务器作为消息代理")
    print("2. 在另一个终端窗口启动Celery Worker:")
    print("   Windows系统: celery -A celery_app worker --loglevel=info --pool=solo -Q celery,datax,high_priority")
    print("   Linux/Mac系统: celery -A celery_app worker --loglevel=info -Q celery,datax,high_priority")
    print("3. 确保DataX已正确安装在项目目录的datax文件夹中")
    print("4. 运行此示例脚本查看效果:")
    print("   python example_usage.py")
    print("=" * 50)