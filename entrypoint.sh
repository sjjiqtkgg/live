#!/bin/bash
# entrypoint.sh —— 双进程监督器（加入 FastAPI 就绪探测，防平台端口探测错乱）
#
# 旧版行为：`wait -n` —— uvicorn 或 douyinLive（Go 抖音弹幕，:1088）任一退出，
# 整个容器跟着退出，依赖平台（Render / docker --restart）把两个进程一起重启。
# 副作用：Go 弹幕服务崩溃会连坐 FastAPI（所有在途请求与弹幕 WS 连接全断），
# 反之亦然——一个组件的故障半径被放大成整个服务栈的故障。
#
# 新版行为：
#   0. **就绪探测（新增）**：先启动 FastAPI，用 curl 死等其就绪，确认后再拉起 Go 服务。
#      防止 Orkestr 等平台在启动瞬间探测到 Go 服务（1088）而误判主端口。
#   1. 独立重启：哪个进程退出只重启哪个（带指数退避 1s→30s 封顶，
#      稳定运行满 60s 退避重置），另一个进程与其连接完全不受影响；
#   2. 信号转发：容器 stop（SIGTERM/SIGINT）→ 先 TERM 两个子进程、
#      宽限 8s 等 uvicorn 优雅排空（WS 连接/在途请求），残留强杀后干净退出。
#
# 依赖：`wait -n -p VAR` 需要 bash >= 5.1（python:3.12-slim 基于 Debian
# bookworm，bash 5.2，满足）；zombie 子进程由 bash 在 wait 时自动回收。
set -u

APP_PORT="${PORT:-10000}"
GO_PORT="${GO_DANMAKU_PORT:-1088}"

start_fastapi() {
  echo "[supervisor] starting FastAPI (uvicorn) on :${APP_PORT}"
  uvicorn main:app --host 0.0.0.0 --port "${APP_PORT}" &
  FASTAPI_PID=$!
  FASTAPI_STARTED=$SECONDS
}

start_douyinlive() {
  echo "[supervisor] starting douyinLive (Go) on :${GO_PORT}"
  /usr/local/bin/douyinLive 2>&1 &
  GO_PID=$!
  GO_STARTED=$SECONDS
}

# ---- 停机处理：信号 → 子进程 TERM → 宽限 → KILL ----
SHUTDOWN=0
term_children() {
  if [ "$SHUTDOWN" -eq 1 ]; then return; fi
  SHUTDOWN=1
  echo "[supervisor] stop signal received, terminating children..."
  [ -n "${FASTAPI_PID:-}" ] && kill -TERM "${FASTAPI_PID}" 2>/dev/null
  [ -n "${GO_PID:-}" ] && kill -TERM "${GO_PID}" 2>/dev/null
}
trap term_children TERM INT

# ---- 每进程独立的崩溃退避 ----
BACKOFF_MIN=1
BACKOFF_MAX=30
STABLE_SECONDS=60
FASTAPI_BACKOFF=$BACKOFF_MIN
GO_BACKOFF=$BACKOFF_MIN

# ================== 启动阶段：先起 FastAPI，再起 Go ==================
start_fastapi

# 阻塞等待，直到 FastAPI 成功在 $APP_PORT 上响应
echo "[supervisor] waiting for FastAPI to be ready on :${APP_PORT}..."
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$SHUTDOWN" -eq 0 ]; do
  # 使用 curl 探测，返回状态码不是 000 即认为网络层已通（哪怕返回 404/403 也算服务活了）
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${APP_PORT}/" || echo "000")
  if [ "$HTTP_CODE" != "000" ]; then
    echo "[supervisor] FastAPI is ready! (HTTP $HTTP_CODE)"
    break
  fi
  sleep 1
  RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "[supervisor] WARNING: FastAPI did not become ready in 30s, starting Go anyway..."
fi

# 确认 FastAPI 起来后，再启动 Go 弹幕服务
start_douyinlive
# ====================================================================

# ---- 主监督循环：任一退出只重启该进程 ----
while :; do
  EXITED_PID=""
  wait -n -p EXITED_PID || true
  [ "$SHUTDOWN" -eq 1 ] && break
  [ -z "$EXITED_PID" ] && continue

  if [ "$EXITED_PID" = "$FASTAPI_PID" ]; then
    RC=0; wait "$FASTAPI_PID" 2>/dev/null; RC=$?
    echo "[supervisor] FastAPI exited (rc=${RC})"
    [ $((SECONDS - FASTAPI_STARTED)) -ge "$STABLE_SECONDS" ] && FASTAPI_BACKOFF=$BACKOFF_MIN
    echo "[supervisor] restarting FastAPI in ${FASTAPI_BACKOFF}s"
    sleep "$FASTAPI_BACKOFF"
    FASTAPI_BACKOFF=$((FASTAPI_BACKOFF * 2))
    [ "$FASTAPI_BACKOFF" -gt "$BACKOFF_MAX" ] && FASTAPI_BACKOFF=$BACKOFF_MAX
    start_fastapi
  elif [ "$EXITED_PID" = "$GO_PID" ]; then
    RC=0; wait "$GO_PID" 2>/dev/null; RC=$?
    echo "[supervisor] douyinLive exited (rc=${RC})"
    [ $((SECONDS - GO_STARTED)) -ge "$STABLE_SECONDS" ] && GO_BACKOFF=$BACKOFF_MIN
    echo "[supervisor] restarting douyinLive in ${GO_BACKOFF}s"
    sleep "$GO_BACKOFF"
    GO_BACKOFF=$((GO_BACKOFF * 2))
    [ "$GO_BACKOFF" -gt "$BACKOFF_MAX" ] && GO_BACKOFF=$BACKOFF_MAX
    start_douyinlive
  fi
done

# ---- 收尾：宽限 8s 等子进程自行退出（uvicorn 优雅排空 WS/请求），残留强杀 ----
is_alive() { [ -n "$1" ] && kill -0 "$1" 2>/dev/null; }
echo "[supervisor] waiting up to 8s for children to exit..."
for _ in $(seq 1 8); do
  if is_alive "${FASTAPI_PID:-}" || is_alive "${GO_PID:-}"; then
    sleep 1
  else
    break
  fi
done
[ -n "${FASTAPI_PID:-}" ] && kill -KILL "$FASTAPI_PID" 2>/dev/null
[ -n "${GO_PID:-}" ] && kill -KILL "$GO_PID" 2>/dev/null
wait 2>/dev/null
echo "[supervisor] all children stopped, exiting cleanly."
exit 0
