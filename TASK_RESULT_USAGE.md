# DataX-Celery 任务结果获取使用说明

本文档介绍了如何获取 DataX-Celery 系统中任务的执行结果。

## 1. 任务类型

DataX-Celery 支持两种主要的任务类型：

1. **execute_datax_job**: 执行 DataX 数据同步作业
2. **validate_datax_job**: 验证 DataX 作业配置文件的有效性

## 2. 获取任务结果的方法

### 2.1 使用 DataXTaskScheduler 类（推荐）

```python
from tasks_scheduler import DataXTaskScheduler

# 创建调度器实例
scheduler = DataXTaskScheduler()

# 方法1: 获取 execute_datax_job 类型任务结果（兼容旧版本）
result = scheduler.get_task_result("your-task-id")

# 方法2: 根据任务类型获取结果
result = scheduler.get_task_result_by_id("your-task-id", "execute")  # 执行任务
result = scheduler.get_task_result_by_id("your-task-id", "validate")  # 验证任务

# 方法3: 直接获取特定类型任务结果
result = scheduler.get_execute_datax_job_result("your-task-id")   # 执行任务
result = scheduler.get_validate_datax_job_result("your-task-id")  # 验证任务
```

### 2.2 任务结果对象的属性和方法

返回的 `result` 对象具有以下有用的属性和方法：

- `result.state`: 任务状态（PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED 等）
- `result.ready()`: 任务是否已完成（成功或失败）
- `result.successful()`: 任务是否成功完成
- `result.failed()`: 任务是否失败
- `result.result`: 任务的实际返回结果

## 3. 不同任务类型的返回结果格式

### 3.1 execute_datax_job 任务

返回一个字典，包含以下键值：

```python
{
    'success': True/False,      # 执行是否成功
    'exit_code': 0,             # DataX 进程退出码
    'stdout': '...',            # 标准输出内容
    'stderr': '...'             # 错误输出内容
}
```

### 3.2 validate_datax_job 任务

返回一个布尔值：

- `True`: 配置文件有效
- `False`: 配置文件无效

## 4. 使用示例

### 4.1 获取 execute_datax_job 任务结果

```python
from tasks_scheduler import DataXTaskScheduler

scheduler = DataXTaskScheduler()
task_id = "08c30376-b3f6-41d3-a90b-053032e823c5"  # 替换为实际的任务ID

result = scheduler.get_execute_datax_job_result(task_id)

print(f"任务状态: {result.state}")

if result.ready():
    execution_result = result.result
    if execution_result.get('success', False):
        print("DataX作业执行成功！")
        print(f"退出码: {execution_result.get('exit_code')}")
        print(f"输出: {execution_result.get('stdout')}")
    else:
        print("DataX作业执行失败！")
        print(f"错误信息: {execution_result.get('stderr')}")
else:
    print("任务仍在执行中...")
```

### 4.2 获取 validate_datax_job 任务结果

```python
from tasks_scheduler import DataXTaskScheduler

scheduler = DataXTaskScheduler()
task_id = "your-validate-task-id"  # 替换为实际的任务ID

result = scheduler.get_validate_datax_job_result(task_id)

print(f"任务状态: {result.state}")

if result.ready():
    is_valid = result.result
    print(f"作业配置验证结果: {'有效' if is_valid else '无效'}")
else:
    print("任务仍在执行中...")
```

## 5. 运行示例脚本

项目中提供了 `get_task_result_example.py` 脚本，可以直接运行来测试任务结果获取功能：

```bash
python get_task_result_example.py
```

记得将脚本中的 `sample_task_id` 替换为实际的任务 ID。
