#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
调试DataX-Celery任务结果格式的脚本
"""

from tasks_scheduler import DataXTaskScheduler
import json

def debug_task_result(task_id: str):
    """
    调试指定任务ID的结果格式
    
    Args:
        task_id: 任务ID
    """
    print(f"=== 调试任务结果格式 ===")
    print(f"任务ID: {task_id}")
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    try:
        # 获取任务结果
        result = scheduler.get_execute_datax_job_result(task_id)
        
        # 输出任务基本信息
        print(f"任务状态: {result.state}")
        print(f"任务是否就绪: {result.ready()}")
        print(f"任务是否成功: {result.successful()}")
        print(f"任务是否失败: {result.failed()}")
        
        # 输出原始结果
        print(f"\n原始结果类型: {type(result.result)}")
        print(f"原始结果值: {result.result}")
        
        # 如果结果是字典，格式化输出
        if isinstance(result.result, dict):
            print(f"\n格式化的结果:")
            print(json.dumps(result.result, indent=2, ensure_ascii=False))
        elif result.result is not None:
            print(f"\n结果字符串表示: {str(result.result)}")
        else:
            print(f"\n结果为None")
            
    except Exception as e:
        print(f"调试任务结果时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 使用实际的任务ID进行调试
    task_id = "08c30376-b3f6-41d3-a90b-053032e823c5"
    debug_task_result(task_id)