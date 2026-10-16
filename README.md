# backend（Render / Docker 部署单元）

唯一后端：`main.py`（FastAPI，`main:app`）= 原有解析/代理/弹幕 WS + `browse_api.py` 挂载的 7 平台浏览接口。

- 启动：`entrypoint.sh` → uvicorn 监听 `0.0.0.0:$PORT`（默认 10000），并拉起 Go `douyinLive`（抖音弹幕，:1088）
- 监督（P3 批）：`entrypoint.sh` 是双进程监督器——任一进程崩溃只重启该进程（指数退避 1s→30s 封顶、稳定 60s 重置），另一个不受连坐；容器 stop 转发 TERM、宽限 8s 优雅收尾
- 非 root（P3 批）：Docker 镜像以专用非特权用户（uid 10001）运行，运行期镜像内无 root 进程；端口 10000/1088 均 >1024 可直接 bind
- 环境变量、部署步骤与健康检查路径见仓库根目录 `README.md` 第一、四节
- Cookie 一律走环境变量或前端请求头（`x-platform-cookie`），不要写进代码
