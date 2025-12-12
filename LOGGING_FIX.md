# DataX-Celery 日志配置修复说明

本文档说明了 DataX-Celery 项目中日志配置的问题及其解决方案。

## 问题描述

在原始代码中，存在以下日志相关问题：

1. `celery_app.log` 文件为空，没有记录任何日志信息
2. `tasks_scheduler.log` 文件为空，没有记录任何日志信息
3. 只有 `datax_executor.log` 文件有日志记录

## 问题原因

经过分析发现，问题的根本原因是：

1. 在多个模块中使用了 `logging.basicConfig()` 方式配置日志，但在 Celery worker 进程中这种方式可能无法正确初始化
2. 日志记录器的配置在不同模块中可能存在冲突或覆盖问题
3. 没有正确处理日志记录器的重复添加问题

## 解决方案

### 1. 修改日志配置方式

在所有三个主要模块中（`celery_app.py`、`datax_executor.py`、`tasks_scheduler.py`），将原来的 `logging.basicConfig()` 配置方式改为手动创建和配置日志记录器：

```python
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
        os.path.join(LOG_DIR, '模块对应的日志文件名'),
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
```

### 2. 确保日志目录存在

在配置日志之前，确保日志目录已经创建：

```python
os.makedirs(LOG_DIR, exist_ok=True)
```

### 3. 清除现有处理器

为了避免日志处理器重复添加的问题，在创建新的处理器之前清除现有的处理器：

```python
logger.handlers.clear()
```

## 涉及的文件

1. `celery_app.py` - Celery 应用主文件
2. `datax_executor.py` - DataX 执行器
3. `tasks_scheduler.py` - 任务调度器

## 验证结果

修复后，所有三个日志文件都能正常记录日志：

- `celery_app.log`: 记录 Celery 应用相关的日志
- `datax_executor.log`: 记录 DataX 执行器相关的日志
- `tasks_scheduler.log`: 记录任务调度器相关的日志

## 使用建议

1. 在启动 Celery worker 之前，确保 Redis 服务正在运行
2. 如果日志仍然不能正常记录，可以手动运行日志测试脚本来验证配置
3. 定期检查日志文件大小，必要时进行日志轮转

## 注意事项

1. 此修复方案针对 Windows 环境下的 Celery 应用进行了优化
2. 如果在 Linux 或其他环境中部署，可能需要根据具体环境调整日志配置
3. 日志级别可以通过 `config.py` 中的 `LOG_LEVEL` 变量进行调整
4. 所有模块使用相同的日志配置模式，确保日志行为的一致性
