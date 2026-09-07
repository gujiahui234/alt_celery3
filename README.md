# alt_celery3

基于 **Celery + Redis（redis-stack）+ Flower + Docker** 的生产级异步任务应用。
支持普通任务与定时任务，任务主体统一放在 `app/tasks/` 子文件夹内，便于持续扩展。

## 功能特性

- **普通任务**：生产者投递、worker 异步执行（示例：`tasks.example.add` 加法任务）。
- **定时任务**：celery-beat 周期调度（示例：`tasks.scheduled.add`，默认每 30 分钟执行一次），
  并在 Redis 中记录最近一次执行的任务 id，可随时查询定时任务结果。
- **批量任务**：`tasks.db.generate_many_students` 多线程批量生成学生（支持生日范围过滤），
  基于 `scdb_mysql_speed` 的 `execute_many` 批量插入 + 每线程独享连接，实测百万级数据
  约 31 秒写入完成（> 3 万行/秒），任务过程通过 `PROGRESS` 状态实时上报进度。
- **AI 任务**：`tasks.ai.get_un_groups` 通过硅基流动（SiliconFlow）Chat Completion API
  获取高校及专业组信息并入库 `web_db.universities` / `major_groups`（按名称查重去重）。
  公用函数 `gjld_chat_completion` 可向大模型提问任意问题并获取纯文本回答
  （自动剥离 DeepSeek 系模型的 `<think>` 推理块）。
- **监控面板**：Flower（同镜像启动，账号密码保护）。
- **消息中间件**：复用已有的、带密码保护的 redis-stack 服务器，通过环境变量注入。
- **生产级容器**：单镜像多服务（worker / beat / flower），镜像内显式创建非特权专用用户 `celeuser`，
  worker 健康检查，beat 调度状态持久化到卷。
- **双依赖清单**：现代 `pyproject.toml` + 传统 `requirements.txt`（Docker 构建使用后者）。
- **更新脚本**：`update.sh` 一键拉取最新代码并重建容器。

## 目录结构

```
alt_celery3/
├── app/
│   ├── __init__.py            # 包说明与版本号
│   ├── config.py              # 环境变量驱动的配置 + beat 调度表
│   ├── celery_app.py          # 共享 Celery 应用实例（worker/beat/producer 共用）
│   ├── sclog_setup.py         # sclog-lite 日志集成（setup/shutdown 生命周期）
│   └── tasks/
│       ├── __init__.py        # 任务注册子包（新增任务模块放这里）
│       ├── example_tasks.py   # 普通任务示例：add
│       ├── scheduled_tasks.py # 定时任务示例：scheduled_add + 最近结果查询
│       ├── db_tasks.py        # MySQL 任务：try_mysql / get_one_student
│       ├── bulk_student_tasks.py  # 批量任务：generate_many_students（多线程百万级）
│       └── ai_tasks.py        # AI 任务：get_un_groups（硅基流动 API 采集高校信息）
├── run_tasks.py               # 生产者 CLI：调用示例任务、查询定时任务结果
├── run_celery.py              # 本地启动 celery（worker/beat/flower）
├── Dockerfile                 # Python 3.13 镜像，专用非特权用户 celeuser
├── docker-compose.yml         # worker + beat + flower 编排
├── update.sh                  # 拉取 GitHub 更新并重建容器
├── pyproject.toml / requirements.txt
└── .env.example               # 环境变量示例
```

## 环境要求

- Python >= 3.13（本地运行时）
- Docker 与 Docker Compose（容器部署时）
- 一台已有的、带密码保护的 **redis-stack** 服务器（同时充当 broker 与结果后端）

## 快速开始

### 1. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，至少修改：
#   CELERY_BROKER_URL=redis://:<密码>@<redis-stack地址>:6379/0
#   CELERY_RESULT_BACKEND=redis://:<密码>@<redis-stack地址>:6379/1
#   FLOWER_BASIC_AUTH=admin:<你的Flower密码>
```

注意：容器内访问宿主机/局域网 redis-stack 时，`.env` 中的地址要写成容器可达的地址
（局域网 IP、`host.docker.internal` 或 redis-stack 的服务名）。

### 2. 本地运行（无需 Docker）

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

# 终端 1：启动 worker
python run_celery.py worker --loglevel=INFO

# 终端 2：启动 beat（定时任务调度器）
python run_celery.py beat --loglevel=INFO

# 终端 3（可选）：启动 Flower 监控面板
python run_celery.py flower --port=5555
```

### 3. Docker 部署

```bash
docker compose up -d --build      # 构建镜像并启动 worker / beat / flower
docker compose ps                 # 查看状态（worker 带 healthcheck）
docker compose logs -f worker     # 跟踪日志
```

| 服务    | 说明                                        |
|---------|---------------------------------------------|
| `worker` | 执行普通与定时任务，`inspect ping` 健康检查 |
| `beat`   | 周期调度器，调度状态持久化到卷 `celery_beat_data` |
| `flower` | 监控面板，映射到宿主机 `${FLOWER_PORT:-5555}`，`FLOWER_BASIC_AUTH` 保护 |

## 使用示例（run_tasks.py）

```bash
# 发送普通加法任务并等待结果
python run_tasks.py add --x 40 --y 2
# [ok] tasks.example.add: state=SUCCESS
#     value: 42

# 查看当前注册的定时任务（beat 调度表）
python run_tasks.py schedules

# 手动触发一次定时任务（不等 beat 周期，便于验证全链路）
python run_tasks.py trigger-scheduled --x 21 --y 21

# 查询定时任务最近一次执行的结果（从 Redis 结果后端读取）
python run_tasks.py latest-scheduled

# 检查 worker 与 broker 连通性
python run_tasks.py ping

# 测试 MySQL web_db 连通性（scdb-mysql-speed 连接池）
python run_tasks.py try-mysql

# 生成一个学生并保存到 web_db.students（class-roster-simulator 模拟数据）
python run_tasks.py student

# 批量生成 10 万名学生（生日限定在 2000-01-01 ~ 2010-12-31）
# 多线程 + execute_many 批量插入，可通过 --threads/--batch-size 调优
python run_tasks.py generate-students --numbers 100000 \
    --birthday-min 2000-01-01 --birthday-max 2010-12-31

# 百万级压测（实测约 31 秒，> 3 万行/秒）
python run_tasks.py generate-students --numbers 1000000 \
    --birthday-min 1995-01-01 --birthday-max 2010-12-31

# 小规模快速验证（eager 模式在本机进程内直接执行，无需 worker）
python run_tasks.py --eager generate-students --numbers 500 \
    --threads 4 --batch-size 100

# 通过硅基流动 LLM API 采集 3 所高校及其专业组信息并入库（按名称查重）
python run_tasks.py get-un-groups --count 3
```

无 broker 的离线演示可用 `--eager`（任务在本地进程内直接执行）：

```bash
python run_tasks.py --eager add --x 1 --y 2
```

## 如何添加新任务

1. 在 `app/tasks/` 下新建模块，例如 `app/tasks/report_tasks.py`：

   ```python
   from app.celery_app import celery_app

   @celery_app.task(name="tasks.report.daily")
   def daily_report() -> str:
       """Generate the daily report.

       Returns:
           A short summary string.
       """
       return "report generated"
   ```

2. 在 `app/celery_app.py` 的 `include` 列表中登记模块 `"app.tasks.report_tasks"`。
3. 若需要定时执行，在 `app/config.py` 的 `build_beat_schedule()` 中增加一条条目
   （可用 `app/config.py` 顶部新增常量作为任务名的唯一事实来源）。
4. 重启 worker 与 beat（或执行 `./update.sh`）。

## 更新部署

服务器上代码有更新时：

```bash
./update.sh
```

脚本会依次：`git pull --ff-only` 拉取最新代码 → `docker compose up -d --build`
重建镜像并平滑重启全部服务 → 输出当前栈状态。

## 配置项（.env）

| 变量 | 说明 | 默认 |
|------|------|------|
| `CELERY_BROKER_URL` | redis-stack broker 连接串（含密码） | `redis://:your-redis-password@127.0.0.1:6379/0` |
| `CELERY_RESULT_BACKEND` | 结果后端连接串（建议不同 DB） | `redis://:your-redis-password@127.0.0.1:6379/1` |
| `CELERY_TIMEZONE` | 时区 | `Asia/Shanghai` |
| `CELERY_RESULT_EXPIRES` | 结果过期秒数 | `86400` |
| `CELERY_TASK_ALWAYS_EAGER` | 本地进程内执行（测试用） | `false` |
| `CELERY_ENABLE_EXAMPLE_BEAT` | 是否注册示例定时任务 | `true` |
| `CELERY_EXAMPLE_BEAT_MINUTES` | 示例定时任务执行间隔（分钟） | `30` |
| `CELERY_WORKER_CONCURRENCY` | worker 并发数（compose 使用） | `2` |
| `FLOWER_PORT` | Flower 宿主机端口 | `5555` |
| `FLOWER_BASIC_AUTH` | Flower 登录账号密码 | `admin:change-me` |
| `MYSQL_WEB_HOST/PORT/USER/PASSWORD/DATABASE` | 业务库 `web_db` 连接信息 | `127.0.0.1/3306/...` |
| `SCLOG_MYSQL_HOST/PORT/USER/PASSWORD/DATABASE/TABLE` | sclog-lite 日志库连接信息 | `127.0.0.1/3306/.../sclog_entries` |
| `SCLOG_MYSQL_ENABLED` | 是否启用 sclog 异步 MySQL 日志后端 | `true` |
| `API_KEY_GJLD` | 硅基流动（SiliconFlow）API-KEY | 无（必填） |
| `BASE_URL` | 硅基流动 OpenAI 兼容端点 | `https://api.siliconflow.cn/v1` |
| `GJLD_MODEL` | AI 任务使用的聊天模型 | `deepseek-ai/DeepSeek-V4-Flash` |

## 自定义包依赖

三个自定义包以 GitHub 直连依赖方式安装（见 `pyproject.toml` / `requirements.txt`）：

| 包 | 导入名 | 用途 |
|----|--------|------|
| `scdb-mysql-speed` | `scdb_mysql_speed` | 高性能 MySQL 客户端（MySQLdb + 连接池，参数化 SQL） |
| `class-roster-simulator` | `class_roster` | 模拟生成中国学生花名册（学号/姓名/性别/出生日期） |
| `sclog-lite` | `sclog_lite` | Loguru 扩展：控制台/轮转文件/异步批量 MySQL 日志 |

`get_one_student` 任务的 `students` 表列与 `class_roster.models.Student`
字段一一对应（`number`/`name`/`gender`/`birthday`），任务首次运行时自动建表。
操作日志通过 sclog-lite 写入控制台、轮转文件与 `SCLOG_MYSQL_*` 指定的日志库；
Celery worker 通过 `worker_process_init` / `worker_shutdown` 信号完成日志的
初始化与 `shutdown()` 刷新。

## 开发

```bash
pip install -e ".[dev]"
pytest        # 测试
ruff check .  # 代码检查
```

- 依赖变更请**同时**更新 `pyproject.toml` 的 `[project.dependencies]` 与 `requirements.txt`。
- 代码注释遵循 Google 风格 Docstrings。
