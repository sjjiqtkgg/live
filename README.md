# 聚合直播（live-aggregator）

多平台直播聚合：虎牙 / 斗鱼 / 哔哩哔哩 / 抖音 / Twitch / SOOP / PandaTV。

| 目录 | 说明 |
|---|---|
| `backend/` | FastAPI 后端：房间解析、流/弹幕代理、浏览目录接口、弹幕 WebSocket 桥接 |
| `web/` | Vue 3 + Vite 前端（ArtPlayer + hls.js + mpegts.js） |
| `worker/` | Cloudflare Worker：海外流/头像代理（内容类型仲裁、逐跳白名单重定向、大小上限） |

## 快速开始

### 后端

```bash
pip install -r backend/requirements.txt
uvicorn main:app --host 0.0.0.0 --port 10000   # 在 backend/ 目录下
# 或：python main.py（读 PORT 环境变量，默认 10000）
```

### 前端

```bash
cd web
bun install --frozen-lockfile
bun run dev        # 本地开发（/api 代理到 http://127.0.0.1:10000，可用 VITE_DEV_API 覆盖）
bun run build      # 产出 web/dist（VITE_API_BASE 构建期注入后端地址；空 = 同源 /api）
```

### Worker

```bash
cd worker
npx wrangler deploy     # 或在 Cloudflare Workers 控制台粘贴 worker.js
```

部署后把 Worker 地址配到后端环境变量 `CF_WORKER_URL`（前端经 `GET /api/config` 获取，
用于海外流/头像分流；未配置时前端退化为直连）。

## 环境变量（backend，均带默认值、可不配置）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `10000` | `python main.py` 启动端口（uvicorn 内部监听 0.0.0.0） |
| `ALLOWED_HOSTS` | 空（不校验） | Host 头白名单（逗号分隔）；配置后非法 Host 一律 400 |
| `ALLOWED_WS_ORIGINS` | 空（不限制） | 弹幕 WebSocket Origin 白名单（逗号分隔） |
| `MAX_TS_SIZE` | `20971520` | TS 切片字节上限；超限直接断连（拒绝交付截断数据） |
| `TRUST_PROXY_HOPS` / `TRUST_PROXY_NETS` | `1` / 内网段 | 反代场景下取真实客户端 IP（限速桶）的信任策略 |
| `PROXY_LIST` / `EXTERNAL_PROXY_LIST` | 空（直连） | 国内 / 海外代理池（逗号分隔；支持 http(s)/socks5） |
| `EXTERNAL_DIRECT_FALLBACK` | `1` | 海外代理全挂后允许直连兜底（海外部署建议保持 1） |
| `PROXY_POOL_DEAD_SECONDS` | `180` | 代理池整体不可达后的直连优先冷静期 |
| `CONNECT_TIMEOUT` | `5` | 连接超时（秒）；read/write 用整体超时，默认 15s |
| `HTTP2_ENABLED` | `1` | 关闭可退 HTTP/1.1（个别代理对 h2 隧道兼容差时用） |
| `VERIFY_TLS` / `INSECURE_TLS_HOSTS` | `1` / 空 | TLS 校验总开关 / 按域名例外（自签证书平台） |
| `CF_WORKER_URL` | 空 | Worker 地址；海外流与头像经其代理 |
| `WS_PUBLIC_BASE` | 空 | 反代/TLS 终止场景覆盖弹幕 WS 对外地址 |
| `HEALTH_TOKEN` | 空 | 配置后启用 `GET /health/detail`（带 `X-Health-Token`，恒定时间比较） |
| `GO_DANMAKU_HOST` / `GO_DANMAKU_PORT` | `localhost` / `1088` | 抖音弹幕 Go 服务地址（不可达时抖音弹幕自动降级） |
| `FOLLOWS_BATCH_TIMEOUT` | `45` | `/api/follows/batch` 整批总期限（秒） |
| `PARSE_TOTAL_TIMEOUT` | `60` | `/api/parse` 单次解析总期限（秒） |
| `HUYA_FORCE_HTTPS` | `1` | 虎牙 FLV 强制 https（个别 CDN 不支持时设 0） |
| `HUYA_COOKIE` / `DOUYU_COOKIE` / `BILI_COOKIE` / `DOUYIN_COOKIE` / `TWITCH_COOKIE` / `SOOP_COOKIE` / `PANDA_COOKIE` | 空 | 各平台登录 Cookie（也可由前端设置页按请求头传入，优先级更高） |

前端构建期：`VITE_API_BASE`（后端地址）、`VITE_DEV_API`（仅 dev server 代理目标）。

## 测试

```bash
# 后端（离线确定性；live 冒烟默认跳过）
pip install -r backend/requirements.txt -r backend/requirements-dev.txt
python -m pytest backend/tests -q -m "not live"
python -m pytest backend/tests -m live -q      # 手动跑真实上游冒烟（不进 CI 门禁）

# Worker（零依赖）
node --test worker/test/*.test.mjs

# 前端
cd web && bun run typecheck && bun run build
```

CI（`.github/workflows/ci.yml`）：前端类型检查/构建 + pyflakes + Worker 单测 + 后端离线测试。

> 环境变量清单按当前源码逐一核对整理；如上游仓库另有正式 README，以合并后的版本为准。
