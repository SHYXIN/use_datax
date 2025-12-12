# DataX-Celery 项目

这是一个使用 Celery 作为任务队列框架来调用和执行 DataX 数据同步工具的 Python 项目。该项目实现了异步任务调度功能，可以通过 Celery worker 进程执行 DataX 的 JSON 配置文件来完成数据迁移任务。

## 项目结构

```
datax-celery/
├── config.py                       # 项目配置文件
├── datax_executor.py               # DataX执行器类
├── celery_app.py                   # Celery应用配置和任务定义
├── tasks_scheduler.py              # 任务调度器类
├── example_usage.py                # 使用示例
├── requirements.txt                # 项目依赖
├── README.md                      # 项目说明文档
├── .gitignore                     # Git忽略文件
├── .vscode/                       # VSCode配置目录
│   ├── settings.json              # VSCode工作区设置
│   └── launch.json                # VSCode调试配置
└── datax/
    └── job/
        ├── job.json               # 默认的DataX作业配置示例
        └── sample_mysql_to_mysql.json  # MySQL到MySQL的数据同步示例
```

## 功能特性

1. **面向对象设计**：采用良好的面向对象编程实践，封装了 DataX 执行器和任务调度器。
2. **异步任务执行**：通过 Celery 实现异步执行 DataX 作业，避免阻塞主线程。
3. **任务调度**：提供便捷的任务调度接口，支持自定义队列和参数。
4. **作业验证**：支持对 DataX 作业配置文件进行验证。
5. **错误处理**：完善的异常处理和重试机制。
6. **日志记录**：详细的日志记录便于调试和监控。

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 启动 Redis 服务器

Celery 需要一个消息代理，这里使用 Redis：

```bash
redis-server
```

### 2. 启动 Celery Worker

**Windows 系统**：

```bash
celery -A celery_app worker --loglevel=info --pool=solo
```

如果要指定特定队列：

```bash
celery -A celery_app worker --loglevel=info --pool=solo -Q datax,high_priority
```

**Linux/Mac 系统**：

```bash
celery -A celery_app worker --loglevel=info
```

如果要指定特定队列：

```bash
celery -A celery_app worker --loglevel=info -Q celery,datax,high_priority
```

> **注意**：Windows 系统必须添加 `--pool=solo` 参数，因为 Windows 不支持默认的 prefork 模式。

### 3. 运行示例

```bash
python example_usage.py
```

### 4. VSCode 开发环境配置

项目已包含完整的 VSCode 配置，可直接在 VSCode 中打开项目并使用以下功能：

1. **调试配置**：已预设多种调试配置，包括：

   - Python: Current File - 调试当前 Python 文件
   - DataX Executor - 运行示例程序
   - Celery Worker - 启动 Celery 工作进程（已针对 Windows 系统配置 `--pool=solo` 参数）
   - Celery Worker with Custom Queue - 启动指定队列的 Celery 工作进程（已针对 Windows 系统配置 `--pool=solo` 参数）

2. **开发环境设置**：
   - 自动激活虚拟环境
   - 代码自动格式化（使用 Black）
   - 代码检查（使用 Pylint）
   - 测试框架支持（Pytest）

直接按 F5 即可选择相应的调试配置运行。

### 5. 使用自定义作业配置

项目提供了两个示例作业配置文件：

1. `datax/job/job.json` - 默认的简单流读写示例
2. `datax/job/sample_mysql_to_mysql.json` - MySQL 到 MySQL 的数据同步示例

> **注意**：任务默认发送到 `celery` 队列，如果您使用自定义队列，请确保启动 Worker 时监听相应的队列。

要使用自定义的作业配置，请修改 `example_usage.py` 中的 `DATAX_JOB_PATH` 或 `SAMPLE_MYSQL_JOB_PATH` 变量指向您的配置文件。

### 6. Git 版本控制

项目包含了完整的 `.gitignore` 文件，已配置忽略以下内容：

- Python 编译文件和缓存（`__pycache__`, `*.pyc` 等）
- 虚拟环境目录（`venv/`, `.venv/` 等）
- 日志文件（`logs/`, `*.log` 等）
- VSCode 配置目录（`.vscode/`）
- 系统文件（`.DS_Store`, `Thumbs.db` 等）
- Celery 相关文件（`celerybeat-schedule`, `celerybeat.pid` 等）

这确保了只有必要的源代码被纳入版本控制。

## 核心组件说明

### DataXExecutor 类

位于 `datax_executor.py` 文件中，负责执行 DataX 作业：

- `execute_job()`：执行指定的 DataX 作业配置文件
- `validate_job_config()`：验证作业配置文件的有效性

### Celery 应用

位于 `celery_app.py` 文件中，定义了两个主要任务：

- `execute_datax_job`：执行 DataX 作业的 Celery 任务
- `validate_datax_job`：验证 DataX 作业配置的 Celery 任务

### DataXTaskScheduler 类

位于 `tasks_scheduler.py` 文件中，提供高级调度接口：

- `schedule_job_execution()`：调度执行 DataX 作业
- `schedule_job_validation()`：调度验证 DataX 作业配置
- `get_task_result()`：获取任务执行结果
- `cancel_task()`：取消任务执行

## 配置说明

在 `config.py` 中可以修改以下配置：

- `DATAX_HOME`：DataX 安装目录
- `DATAX_PY_PATH`：DataX 执行脚本路径
- `CELERY_BROKER_URL`：Celery 消息代理 URL
- `CELERY_RESULT_BACKEND`：Celery 结果存储后端
- `LOG_LEVEL`：日志级别
- `LOG_DIR`：日志文件存储目录（默认为项目根目录下的 `logs/` 目录）

日志文件会分别存储在以下文件中：

- `logs/datax_executor.log`：DataX 执行器日志
- `logs/celery_app.log`：Celery 应用日志
- `logs/tasks_scheduler.log`：任务调度器日志

日志同时会输出到控制台，方便开发调试。

## 扩展建议

1. 添加更多的 DataX 参数支持
2. 实现任务进度监控功能
3. 添加 Web 管理界面
4. 支持定时任务调度
5. 添加任务依赖关系管理

## 注意事项

1. 确保 DataX 正确安装并在指定路径下
2. 确保 Redis 服务器正常运行
3. 根据实际环境调整配置参数
4. 使用 MySQL 等数据库作业时，请确保已安装相应的 JDBC 驱动
5. 作业配置文件中的数据库连接信息需要根据实际情况修改

## 日志查看

日志文件存储在项目根目录的 `logs/` 目录下，包含三个不同的日志文件：

- `datax_executor.log`：记录 DataX 执行器的操作日志
- `celery_app.log`：记录 Celery 应用的任务处理日志
- `tasks_scheduler.log`：记录任务调度器的操作日志

您可以通过以下方式查看日志：

```bash
# 实时查看日志
tail -f logs/datax_executor.log

# 或在 Windows 上使用 PowerShell
Get-Content logs\datax_executor.log -Wait
```

## 故障排除

### Redis 连接错误

如果遇到类似以下错误：

```
TypeError: Connection.__init__() got an unexpected keyword argument 'credential_provider'
```

请尝试更新 Redis 依赖包：

```bash
pip uninstall redis
pip install redis==4.3.4
```

或者更新整个依赖环境：

```bash
pip install -r requirements.txt
```

### Windows 系统兼容性问题

在 Windows 系统上运行时，请确保使用了`--pool=solo`参数，如：

```bash
celery -A celery_app worker --loglevel=info --pool=solo
```

python ./datax/bin/datax.py ./datax/job/job.json
python ./datax/bin/datax.py ./datax/job/datax_14_t_day_to_12_t_day-20251202103452.json
