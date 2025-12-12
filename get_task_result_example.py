#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
获取DataX-Celery任务结果的示例脚本
"""

from tasks_scheduler import DataXTaskScheduler

def get_task_result_example(task_id: str, task_type: str = "execute"):
    """
    获取指定任务ID的执行结果示例
    
    Args:
        task_id: 任务ID (例如: 08c30376-b3f6-41d3-a90b-053032e823c5)
        task_type: 任务类型，"execute" 或 "validate"
    """
    print(f"=== 获取任务结果示例 ===")
    print(f"任务ID: {task_id}")
    print(f"任务类型: {task_type}")
    
    # 创建任务调度器实例
    scheduler = DataXTaskScheduler()
    
    try:
        # 根据任务类型获取结果
        if task_type == "execute":
            result = scheduler.get_execute_datax_job_result(task_id)
        elif task_type == "validate":
            result = scheduler.get_validate_datax_job_result(task_id)
        else:
            result = scheduler.get_task_result_by_id(task_id, task_type)
        
        # 输出任务状态信息
        print(f"任务状态: {result.state}")
        
        # 如果任务已完成，输出结果
        if result.ready():
            print(f"任务执行结果: {result.result}")
            
            # 对于execute类型的任务，进一步分析结果
            if task_type == "execute":
                execution_result = result.result
                # 检查execution_result是否为字典且不为None
                if isinstance(execution_result, dict) and execution_result:
                    if execution_result.get('success', False):
                        print("DataX作业执行成功！")
                        # 处理可能为None的字段，注意字段名是return_code而不是exit_code
                        exit_code = execution_result.get('return_code', 'N/A')
                        stdout = execution_result.get('stdout')
                        stderr = execution_result.get('stderr')
                        
                        print(f"退出码: {exit_code}")
                        if stdout:
                            print(f"标准输出: {str(stdout)[:200]}...")
                        if stderr:
                            # 截取stderr的前1000个字符，因为可能很长
                            print(f"错误输出: {str(stderr)[:1000]}...")
                            if len(str(stderr)) > 1000:
                                print("... (错误输出已截断)")
                    else:
                        print("DataX作业执行失败！")
                        stderr = execution_result.get('stderr')
                        if stderr:
                            print(f"错误信息: {stderr}")
                        else:
                            print("无错误信息")
                else:
                    print("任务执行结果格式不符合预期")
            
            # 对于validate类型的任务，输出验证结果
            elif task_type == "validate":
                is_valid = result.result
                print(f"作业配置验证结果: {'有效' if is_valid else '无效'}")
        else:
            print("任务仍在执行中...")
            
    except Exception as e:
        print(f"获取任务结果时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 示例用法
    # 替换为您的实际任务ID
    sample_task_id = "08c30376-b3f6-41d3-a90b-053032e823c5"
    
    # 获取execute类型任务的结果
    get_task_result_example(sample_task_id, "execute")
    
    print("\n" + "="*50)
    
    # 如果需要获取validate类型任务的结果，可以这样调用：
    # get_task_result_example(sample_task_id, "validate")