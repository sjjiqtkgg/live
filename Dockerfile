# ---- Stage 1: 从官方镜像提取 douyinLive 二进制 ----
FROM ghcr.io/jwwsjlm/douyinlive:v2.3.1 AS douyinlive

# ---- Stage 2: 构建我们的 Python 服务 ----
# 【P1二轮：版本对齐】3.11-slim → 3.12-slim：与 CI（.github/workflows/ci.yml
# python-version: "3.12"）及本地开发环境对齐到同一版本；3.12-slim 同为
# bookworm（bash 5.2，entrypoint.sh 的 wait -n -p 依赖 bash 5.1+ 仍满足）。
FROM python:3.12-slim

# 安装系统工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 从官方镜像中复制 douyinLive 二进制
COPY --from=douyinlive /app/douyinLive /usr/local/bin/douyinLive

# 复制应用代码和启动脚本
COPY . .
COPY entrypoint.sh /entrypoint.sh

# 【P3·非 root】整个服务栈改用专用非特权用户运行：
#   - 两个监听端口（$PORT=10000 与 Go 弹幕 1088）都 > 1024，非特权用户可直接 bind；
#   - 运行时不落任何文件（缓存全内存），唯一潜在写点是 __pycache__，直接
#     用 PYTHONDONTWRITEBYTECODE=1 关闭（写失败本就只降速不报错，关闭后冷启动
#     的编译开销远小于每次容器拉起，且避免给 /app 开写权限的攻击面）；
#   - 所有文件属主调整在 root 构建期一次完成，运行期镜像内无 root 进程。
RUN useradd --system --uid 10001 --create-home --user-group appuser \
    && chown -R appuser:appuser /app \
    && chmod +x /entrypoint.sh /usr/local/bin/douyinLive
USER appuser:appuser

# Render injects PORT at runtime (default Web Service port is 10000).
# PYTHONUNBUFFERED=1：uvicorn 日志不落盘、直接 stdout，行缓冲保证平台日志实时可见。
ENV PORT=10000 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
EXPOSE 10000

# entrypoint.sh 是双进程监督器（P3 批重写）：FastAPI 与 douyinLive 崩溃互不连坐，
# 各自带退避独立重启；容器 stop 时转发 TERM 并干净收尾。见脚本头注释。
CMD ["/entrypoint.sh"]
