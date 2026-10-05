#!/usr/bin/env bash
# 上面这一行叫 shebang：告诉类 Unix 环境用 Bash 解释本文件；Windows 下也可显式用 bash.exe 运行。
# 本脚本负责启动后端 API、异步 Worker 和前端开发服务器，并在退出时清理这些进程。
# -e：命令失败就退出；-u：使用未定义变量时报错；pipefail：管道中任一命令失败即视为失败。
# 这能让前台初始化命令失败时及时退出；后台进程的错误仍需看运行日志。“|| true”表示明确忽略该步失败。
set -euo pipefail

# $0 是脚本路径；dirname 取所在目录，cd 后 pwd 得到绝对路径。
# CDPATH= 避免用户的 CDPATH 设置影响 cd；因此从任何工作目录调用脚本都能找到项目文件。
ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$ROOT_DIR"

# ${变量:-默认值} 表示变量未设置或为空时采用默认值；运行配置中的环境变量可以覆盖这些路径。
BACKEND_DIR="${BACKEND_DIR:-$ROOT_DIR/backend}"
FRONTEND_DIR="${FRONTEND_DIR:-$ROOT_DIR/frontend}"
RUN_DIR="${RUN_DIR:-$BACKEND_DIR/.run}"

# 启动开关：1=启动，0=跳过。默认启动 API、Worker、前端；不自动启动 Redis 或 RabbitMQ。
# 注意：Worker 需要可用的 RabbitMQ；若只想运行前后端，可设置 START_WORKER=0。
START_REDIS="${START_REDIS:-0}"
START_RABBITMQ="${START_RABBITMQ:-0}"
START_BACKEND="${START_BACKEND:-1}"
START_WORKER="${START_WORKER:-1}"
START_FRONTEND="${START_FRONTEND:-1}"

# 可选的 Docker Compose 设置：只有 START_RABBITMQ=1 才尝试用 Compose 启动 RabbitMQ，默认值是 0。
# COMPOSE_FILE 可指定编排文件；STOP_DOCKER=1 表示脚本退出时停止 Compose 服务，默认不停止。
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
STOP_DOCKER="${STOP_DOCKER:-0}"

# 前端设置：FRONTEND_INSTALL=auto 仅在缺少 node_modules 时执行 npm install；1=每次安装，0=跳过。
# FRONTEND_SCRIPT 默认 dev，即执行 npm run dev；也可以改为 package.json 中的其他脚本名。
FRONTEND_INSTALL="${FRONTEND_INSTALL:-auto}"
FRONTEND_SCRIPT="${FRONTEND_SCRIPT:-dev}"

# 本地 Redis 设置：只有 START_REDIS=1 才尝试启动；若 redis-cli 检测到服务已运行，就跳过。
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_CONF="${REDIS_CONF:-}"

# 函数定义格式是 函数名() { ... }。$1、$2 分别是调用函数时传入的第一个、第二个参数。
# 启动前先检查目录和命令，避免子进程启动后才发现依赖缺失。
require_dir() {
  if [ ! -d "$2" ]; then
    echo "[start.sh] $1 dir not found: $2"
    exit 1
  fi
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[start.sh] command not found: $1"
    exit 1
  fi
}

# 记录后台进程的 PID（进程号），以便按 Ctrl+C 或脚本退出时停止它们。
BACKEND_PID=""
WORKER_PID=""
FRONTEND_PID=""
# 清理函数必须容忍“进程已经退出”等情况，因此先 set +e，并用 || true 忽略单个停止操作的失败。
cleanup() {
  set +e
  if [ -n "${FRONTEND_PID:-}" ]; then
    echo "[start.sh] Stopping frontend (pid=$FRONTEND_PID)"
    kill "$FRONTEND_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${WORKER_PID:-}" ]; then
    echo "[start.sh] Stopping worker (pid=$WORKER_PID)"
    kill "$WORKER_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${BACKEND_PID:-}" ]; then
    echo "[start.sh] Stopping backend (pid=$BACKEND_PID)"
    kill "$BACKEND_PID" >/dev/null 2>&1 || true
  fi
  # Windows Git Bash 下，kill 有时不能可靠地停止子进程树；可用时再用 taskkill /T /F 收尾。
  if command -v taskkill >/dev/null 2>&1; then
    if [ -n "${FRONTEND_PID:-}" ]; then
      taskkill //PID "$FRONTEND_PID" //T //F >/dev/null 2>&1 || true
    fi
    if [ -n "${WORKER_PID:-}" ]; then
      taskkill //PID "$WORKER_PID" //T //F >/dev/null 2>&1 || true
    fi
    if [ -n "${BACKEND_PID:-}" ]; then
      taskkill //PID "$BACKEND_PID" //T //F >/dev/null 2>&1 || true
    fi
  fi

  if [ "$STOP_DOCKER" = "1" ] && [ -n "${COMPOSE_CMD:-}" ] && [ -f "$COMPOSE_FILE" ]; then
    echo "[start.sh] Stopping docker compose services"
    $COMPOSE_CMD -f "$COMPOSE_FILE" stop >/dev/null 2>&1 || true
  fi
}
# trap 将清理函数绑定到中断、终止和正常退出；脚本结束时尝试停止前后端。
trap cleanup INT TERM EXIT

# [ 条件 ] 是 Bash 的条件测试；|| 表示“或”。需要运行 Go 进程时才检查 Go 和后端目录。
if [ "$START_BACKEND" = "1" ] || [ "$START_WORKER" = "1" ]; then
  require_dir backend "$BACKEND_DIR"
  mkdir -p "$RUN_DIR"
  require_cmd go
fi

if [ "$START_FRONTEND" = "1" ]; then
  require_dir frontend "$FRONTEND_DIR"
  mkdir -p "$RUN_DIR"
  require_cmd npm
fi

# 检测新旧两种 Compose 命令。只在 START_RABBITMQ=1 时才会调用它。
detect_compose() {
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      COMPOSE_CMD="docker compose"
      return 0
    fi
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
    return 0
  fi
  return 1
}

start_rabbitmq_compose() {
  if [ ! -f "$COMPOSE_FILE" ]; then
    echo "[start.sh] $COMPOSE_FILE not found; skip starting RabbitMQ via docker compose"
    return 0
  fi
  if ! detect_compose; then
    echo "[start.sh] docker compose not found; skip starting RabbitMQ via docker compose"
    return 0
  fi

  echo "[start.sh] Starting RabbitMQ via docker compose ($COMPOSE_FILE)"
  $COMPOSE_CMD -f "$COMPOSE_FILE" up -d rabbitmq

  # 尽力等待 RabbitMQ 就绪，最多检查 30 次；超时后仍继续，连接结果由后端/Worker 自行报告。
  if command -v docker >/dev/null 2>&1; then
    local rabbit_cid=""
    rabbit_cid="$($COMPOSE_CMD -f "$COMPOSE_FILE" ps -q rabbitmq 2>/dev/null || true)"
    if [ -z "$rabbit_cid" ]; then
      rabbit_cid="my-rabbitmq"
    fi

    local i=0
    while [ "$i" -lt 30 ]; do
      if docker exec "$rabbit_cid" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        echo "[start.sh] RabbitMQ ready"
        return 0
      fi
      sleep 1
      i=$((i + 1))
    done
    echo "[start.sh] RabbitMQ may not be ready yet; continuing anyway"
  fi
}

# 如需由脚本启动本机 Redis，日志写入 .run/redis.log，进程号写入 redis.pid。
start_redis() {
  if ! command -v redis-server >/dev/null 2>&1; then
    echo "[start.sh] redis-server not found; skip starting Redis"
    return 0
  fi

  if command -v redis-cli >/dev/null 2>&1; then
    if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping >/dev/null 2>&1; then
      echo "[start.sh] Redis already running at $REDIS_HOST:$REDIS_PORT"
      return 0
    fi
  fi

  echo "[start.sh] Starting Redis at $REDIS_HOST:$REDIS_PORT"
  if [ -n "$REDIS_CONF" ]; then
    nohup redis-server "$REDIS_CONF" >"$RUN_DIR/redis.log" 2>&1 &
  else
    nohup redis-server --bind "$REDIS_HOST" --port "$REDIS_PORT" >"$RUN_DIR/redis.log" 2>&1 &
  fi
  echo $! >"$RUN_DIR/redis.pid"
}

# (...) 在子 Shell 中运行：只让后端进程切到 backend 目录，不改变主脚本的工作目录。
# 末尾的 & 让命令在后台执行；$! 是刚启动的后台任务 PID。
start_backend_bg() {
  echo "[start.sh] Starting backend (background)"
  (cd "$BACKEND_DIR" && go run ./cmd) &
  BACKEND_PID=$!
  echo "$BACKEND_PID" >"$RUN_DIR/backend.pid"
  echo "[start.sh] Backend PID: $BACKEND_PID"
}

start_worker_bg() {
  echo "[start.sh] Starting worker (background)"
  (cd "$BACKEND_DIR" && go run ./cmd/worker) &
  WORKER_PID=$!
  echo "$WORKER_PID" >"$RUN_DIR/worker.pid"
  echo "[start.sh] Worker PID: $WORKER_PID"
}

# 前端依赖按需安装，然后在 frontend 目录运行 npm 脚本；安装失败会因 set -e 中止启动。
start_frontend_bg() {
  if [ "$FRONTEND_INSTALL" = "1" ] || { [ "$FRONTEND_INSTALL" = "auto" ] && [ ! -d "$FRONTEND_DIR/node_modules" ]; }; then
    echo "[start.sh] Installing frontend deps"
    (cd "$FRONTEND_DIR" && npm install)
  fi

  echo "[start.sh] Starting frontend (background, npm run $FRONTEND_SCRIPT)"
  (cd "$FRONTEND_DIR" && npm run "$FRONTEND_SCRIPT") &
  FRONTEND_PID=$!
  echo "$FRONTEND_PID" >"$RUN_DIR/frontend.pid"
  echo "[start.sh] Frontend PID: $FRONTEND_PID"
}

# 最后按开关依次启动可选依赖和三个应用进程；&& 表示“且”，{ ...; } 用来分组条件。
if [ "$START_RABBITMQ" = "1" ] && { [ "$START_BACKEND" = "1" ] || [ "$START_WORKER" = "1" ]; }; then
  start_rabbitmq_compose
fi

if [ "$START_REDIS" = "1" ] && { [ "$START_BACKEND" = "1" ] || [ "$START_WORKER" = "1" ]; }; then
  start_redis
fi

if [ "$START_BACKEND" = "1" ]; then
  start_backend_bg
fi

if [ "$START_WORKER" = "1" ]; then
  start_worker_bg
fi

if [ "$START_FRONTEND" = "1" ]; then
  start_frontend_bg
fi

if [ "$START_BACKEND" = "1" ] || [ "$START_WORKER" = "1" ] || [ "$START_FRONTEND" = "1" ]; then
  echo "[start.sh] Press Ctrl+C to stop."
  # wait 让脚本保持运行并等待后台任务；否则主脚本会马上退出，触发上面的 cleanup。
  wait
else
  echo "[start.sh] Nothing to start. Set START_BACKEND=1 and/or START_WORKER=1 and/or START_FRONTEND=1."
fi
