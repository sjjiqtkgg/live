import copy
import json
import re
import os
import httpx
import asyncio
import itertools
import websockets
import time
import hashlib
import hmac
import threading
import base64
import ipaddress
import random
import secrets
import logging
from fastapi import FastAPI, Query, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse, Response, JSONResponse
from urllib.parse import unquote, urlparse, quote, urljoin, urlunparse

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

# -------------------- 日志配置 --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# 【P0日志泄露】httpx/httpcore 的 INFO 日志会把完整请求 URL（含鉴权 token、OAuth secret）
# 原样打进日志，比 _short_url 截断泄露更彻底；统一降到 WARNING（连接层异常仍可见）
for _noisy_logger in ("httpx", "httpcore"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

try:
    from streamget import DouyinLiveStream
except ImportError:
    DouyinLiveStream = None
    logging.warning("streamget 未安装，抖音平台不可用")

from contextlib import asynccontextmanager

# ==================== Go 弹幕服务配置 ====================
GO_DANMAKU_HOST = os.getenv("GO_DANMAKU_HOST", "localhost")
GO_DANMAKU_PORT = os.getenv("GO_DANMAKU_PORT", "1088")
GO_DANMAKU_BASE = f"ws://{GO_DANMAKU_HOST}:{GO_DANMAKU_PORT}"

_GO_SERVICE_AVAILABLE = False
_GO_SERVICE_LAST_CHECK = 0.0
_GO_SERVICE_CHECK_INTERVAL = 30.0
_GO_SERVICE_CHECK_LOCK = asyncio.Lock()

async def _check_go_service() -> bool:
    """TCP 探测 Go 弹幕服务是否在线，结果写入全局标志。"""
    global _GO_SERVICE_AVAILABLE, _GO_SERVICE_LAST_CHECK
    now = time.time()
    if now - _GO_SERVICE_LAST_CHECK < _GO_SERVICE_CHECK_INTERVAL:
        return _GO_SERVICE_AVAILABLE
    async with _GO_SERVICE_CHECK_LOCK:
        now = time.time()
        if now - _GO_SERVICE_LAST_CHECK < _GO_SERVICE_CHECK_INTERVAL:
            return _GO_SERVICE_AVAILABLE
        _GO_SERVICE_LAST_CHECK = now
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(GO_DANMAKU_HOST, int(GO_DANMAKU_PORT)),
                timeout=2.0
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            if not _GO_SERVICE_AVAILABLE:
                logging.info(f"[Go弹幕] 服务已恢复: {GO_DANMAKU_BASE}")
            _GO_SERVICE_AVAILABLE = True
        except Exception:
            if _GO_SERVICE_AVAILABLE:
                logging.warning(f"[Go弹幕] 服务不可达: {GO_DANMAKU_BASE}，将停止接受抖音弹幕连接")
            _GO_SERVICE_AVAILABLE = False
        return _GO_SERVICE_AVAILABLE

@asynccontextmanager
async def lifespan(app):
    await _check_go_service()

    last_pool_rebuild = time.time()
    last_health_reset = time.time()

    async def _cache_cleanup():
        nonlocal last_pool_rebuild, last_health_reset
        while True:
            await asyncio.sleep(60)
            # 【健壮性】单轮异常保护：原先循环体裸奔——任何一处抛异常（如
            # _check_go_service 的网络抖动）任务即静默死亡，缓存清理/连接池清理/
            # Go 探活全部停摆到进程重启。单轮失败吞掉并继续下一轮。
            try:
                now = time.time()

                # v 可能被异常路径写坏（非 dict），一并剔除，避免 .get 崩掉清理任务（E27）
                expired = [k for k, v in list(M3U8_CACHE.items())
                           if not isinstance(v, dict) or v.get('expire', 0) < now]
                for k in expired:
                    M3U8_CACHE.pop(k, None)
                if expired:
                    logging.info(f"[缓存] 清理 {len(expired)} 条过期条目，剩余 {len(M3U8_CACHE)} 条")
                if len(M3U8_CACHE) > 2000:
                    # 总量兜底：dict 保插入序，从最旧条目开始删（近似 LRU），防长期运行内存膨胀（E22）
                    for k in list(M3U8_CACHE.keys())[:500]:
                        M3U8_CACHE.pop(k, None)
                    logging.info(f"[缓存] M3U8_CACHE 总量超限，已从最旧清理 500 条，剩余 {len(M3U8_CACHE)} 条")

                # TS 代理粘性映射：每轮主动清 10 分钟未使用的条目（活跃流会持续刷新时间戳不受影响）
                # 而不是等总量超 500 才被动清理（E22）
                stale_keys = [k for k, ts in list(STREAM_PROXY_MAP_TS.items()) if now - ts > 600]
                for k in stale_keys:
                    STREAM_PROXY_MAP.pop(k, None)
                    STREAM_PROXY_MAP_TS.pop(k, None)
                if len(STREAM_PROXY_MAP) > 500:
                    now_ts = time.time()
                    safe_to_remove = [
                        k for k in list(STREAM_PROXY_MAP.keys())
                        if now_ts - STREAM_PROXY_MAP_TS.get(k, 0) > 60
                    ]
                    for k in safe_to_remove[:250]:
                        STREAM_PROXY_MAP.pop(k, None)
                        STREAM_PROXY_MAP_TS.pop(k, None)
                    logging.info(f"[缓存] STREAM_PROXY_MAP 超限，已安全清理 {len(safe_to_remove[:250])} 条，剩余 {len(STREAM_PROXY_MAP)} 条")

                if len(_SOOP_OFFLINE_COUNT) > 1000:
                    keys = list(_SOOP_OFFLINE_COUNT.keys())
                    for k in keys[:500]:
                        _SOOP_OFFLINE_COUNT.pop(k, None)

                if len(_DOUYIN_NO_STREAM_LOGGED) > 1000:
                    keys = list(_DOUYIN_NO_STREAM_LOGGED.keys())
                    for k in keys[:500]:
                        _DOUYIN_NO_STREAM_LOGGED.pop(k, None)

                if now - last_pool_rebuild >= 3600:
                    last_pool_rebuild = now
                    async with CLIENT_LOCK:
                        idle_keys = [
                            k for k, (client, last_use) in list(CLIENT_POOL.items())
                            if now - last_use > 900
                        ]
                        for k in idle_keys:
                            entry = CLIENT_POOL.pop(k, None)
                            if entry:
                                client, _ = entry
                                try:
                                    await client.aclose()
                                except Exception:
                                    pass
                    if idle_keys:
                        logging.info(f"[连接池] 惰性清理 {len(idle_keys)} 个空闲连接，剩余 {len(CLIENT_POOL)} 个")

                if now - last_health_reset >= 6 * 3600:
                    last_health_reset = now
                    if _PROXY_HEALTH:
                        logging.info(f"[代理健康] 周期性重置统计，重置前共 {len(_PROXY_HEALTH)} 个代理记录")
                        _PROXY_HEALTH.clear()

                if not _GO_SERVICE_AVAILABLE:
                    await _check_go_service()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("[后台] 缓存清理单轮异常（已吞并继续）")

    task = asyncio.create_task(_cache_cleanup())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    async with CLIENT_LOCK:
        for k, (client, _) in list(CLIENT_POOL.items()):
            try:
                await client.aclose()
            except Exception:
                pass
        CLIENT_POOL.clear()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=500)

# ==================== 安全配置 ====================
ALLOWED_HOSTS = [h.strip().lower() for h in os.getenv("ALLOWED_HOSTS", "").split(",") if h.strip()]
ALLOWED_WS_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_WS_ORIGINS", "").split(",") if o.strip()]
MAX_TS_SIZE = int(os.getenv("MAX_TS_SIZE", str(20 * 1024 * 1024)))


class _TsSegmentTooLarge(RuntimeError):
    """TS 切片超过 MAX_TS_SIZE：拒绝交付截断数据，直接断开响应（见 _stream_and_close）。"""

if ALLOWED_HOSTS:
    logging.info(f"[安全] Host 白名单已启用: {ALLOWED_HOSTS}")
if ALLOWED_WS_ORIGINS:
    logging.info(f"[安全] WebSocket Origin 白名单已启用: {ALLOWED_WS_ORIGINS}")
logging.info(f"[安全] TS 切片大小上限: {MAX_TS_SIZE} 字节")

class _HostHeaderGuardMiddleware:
    """纯 ASGI Host 头守卫（替代旧 @app.middleware("http") 的 BaseHTTPMiddleware 版）。

    【P1：TS 截断改断连的必要条件】BaseHTTPMiddleware 会把流式响应里的应用异常
    吞成「干净收尾」——客户端拿到看似完整、实则截断的 body，TS 超限断连对播放器
    的错误信号会被它整个抹掉（实测：纯 ASGI 中间件栈下断连 → RemoteProtocolError；
    BaseHTTPMiddleware 下同样代码 → 200 + 干净收尾）。纯 ASGI 版不包装 body 流，
    异常直达 uvicorn → transport 直接关闭 → 客户端收到网络错误（可检错、可重试）。
    守卫语义与旧版一致：非法 Host 头 → 400 JSON。
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and ALLOWED_HOSTS:
            raw_host = ""
            for k, v in scope.get("headers") or []:
                if k == b"host":
                    raw_host = v.decode("latin-1")
                    break
            host = raw_host.split(":")[0].strip().lower()
            if host and host not in ALLOWED_HOSTS:
                logging.warning(f"[安全] 拒绝非法 Host 头: {raw_host!r}")
                await JSONResponse({"error": "invalid host header"}, status_code=400)(scope, receive, send)
                return
        await self.app(scope, receive, send)


# 注意 add_middleware 的顺序语义：后加的在最外层。此处与旧装饰器位次一致（最外层），
# Host 校验先于 CORS/GZip/路由发生。
app.add_middleware(_HostHeaderGuardMiddleware)

# 【P0限速伪造】只信反代追加的那一跳：取 XFF 从右数第 TRUST_PROXY_HOPS 项（不再取客户端
# 可随意伪造的第一项）；且仅当直连对端 IP 属于可信反代网段时才参考 XFF，其余场景一律用对端 IP。
# TRUST_PROXY_HOPS=0 完全不信任 XFF（本地直连调试）；TRUST_PROXY_NETS=0.0.0.0/0 信任任意上层（公网 LB）
TRUST_PROXY_HOPS = int(os.getenv("TRUST_PROXY_HOPS", "1"))
TRUST_PROXY_NETS = os.getenv("TRUST_PROXY_NETS", "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1")

def _peer_trusted_proxy(ip: str) -> bool:
    if TRUST_PROXY_NETS.strip() in ("*", "0.0.0.0/0"):
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for net in (n.strip() for n in TRUST_PROXY_NETS.split(",") if n.strip()):
        try:
            if addr in ipaddress.ip_network(net):
                return True
        except ValueError:
            continue
    return False

# 【限速共用桶告警】对端不在 TRUST_PROXY_NETS 时忽略 XFF、按对端 IP 限速：若前面其实
# 是个没登记的负载均衡，全站用户会合并进同一个限速桶（60 次/分钟全站共享）。运行期一旦
# 观察到「同一对端 IP 持续携带多个不同 XFF 值」（正常直连客户端不会这样），打一条告警
# 提示把该反代网段加入 TRUST_PROXY_NETS。每个对端只告警一次，避免刷日志。
_SHARED_BUCKET_XFF_SEEN: dict = {}
_SHARED_BUCKET_WARNED: set = set()
_SHARED_BUCKET_LOCK = threading.Lock()
_SHARED_BUCKET_WARN_THRESHOLD = 5  # 同一对端看到 ≥5 个不同 XFF 首跳才告警（防偶然碰撞）
_SHARED_BUCKET_MAX_TRACKED = 4096  # 观察表上限（防内存增长）

def _watch_shared_rate_limit_bucket(request: Request, client_ip: str) -> None:
    if client_ip in _SHARED_BUCKET_WARNED or len(_SHARED_BUCKET_XFF_SEEN) > _SHARED_BUCKET_MAX_TRACKED:
        return
    xff_raw = request.headers.get("X-Forwarded-For", "")
    first_hop = xff_raw.split(",")[0].strip() if xff_raw else ""
    if not first_hop:
        return
    with _SHARED_BUCKET_LOCK:
        seen = _SHARED_BUCKET_XFF_SEEN.setdefault(client_ip, set())
        seen.add(first_hop)
        if len(seen) < _SHARED_BUCKET_WARN_THRESHOLD:
            return
        _SHARED_BUCKET_WARNED.add(client_ip)
        count = len(seen)
    logging.warning(
        f"[限速] 对端 {client_ip} 不在 TRUST_PROXY_NETS 却已携带 {count} 个不同 XFF 值，"
        f"疑似未登记的负载均衡/反代：当前所有请求按该对端 IP 合并限速（全站共用一个限速桶）。"
        f"若确为反代，请把其网段加入 TRUST_PROXY_NETS 后重启。"
    )

def get_real_ip(request: Request) -> str:
    client_ip = request.client.host if request.client else "unknown"
    if TRUST_PROXY_HOPS <= 0 or not _peer_trusted_proxy(client_ip):
        _watch_shared_rate_limit_bucket(request, client_ip)
        return client_ip
    xff = [x.strip() for x in request.headers.get("X-Forwarded-For", "").split(",") if x.strip()]
    if len(xff) < TRUST_PROXY_HOPS:
        return client_ip
    return xff[-TRUST_PROXY_HOPS]

if SLOWAPI_AVAILABLE:
    limiter = Limiter(key_func=get_real_ip)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
else:
    logging.warning("[限流] slowapi 未安装，/api/parse 与 /api/proxy 不受限流保护")
    class _NoopLimiter:
        def limit(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    limiter = _NoopLimiter()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
MOBILE_UA = "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36"

# ==================== 代理配置 ====================
PROXY_LIST_STR = os.getenv("PROXY_LIST", "")
PROXY_URLS = [p.strip() for p in PROXY_LIST_STR.split(",") if p.strip()] if PROXY_LIST_STR else [None]

EXTERNAL_PROXY_LIST_STR = os.getenv("EXTERNAL_PROXY_LIST", "")
EXTERNAL_PROXY_URLS = [p.strip() for p in EXTERNAL_PROXY_LIST_STR.split(",") if p.strip()] if EXTERNAL_PROXY_LIST_STR else [None]

def _mask_proxy_url(p: str | None) -> str:
    """【P0凭据泄露】日志/健康检查只输出脱敏后的代理地址（隐藏 URL 内嵌账号密码，保留 host:port 便于区分）。"""
    if not p:
        return "直连"
    try:
        u = urlparse(p)
        if u.username or u.password:
            netloc = u.hostname or ""
            if u.port:
                netloc += f":{u.port}"
            return urlunparse((u.scheme or "http", netloc, u.path, "", "", ""))
        return p
    except Exception:
        return "<invalid-proxy>"

# 【P0凭据泄露·补2】通用 userinfo 剥离规则：不依赖“配置里登记过的代理”。
# 用户实测：未登记的 http://a:pw2@h:1_t7.5_v1 走兜底时账号密码原样输出（旧兜底
# _scrub_proxy_secrets 只认 PROXY_URLS/EXTERNAL_PROXY_URLS 登记过的字面量）。
# 【P0凭据泄露·补3】userinfo 匹配改「贪婪到最后一个 @」：用户实测密码里带裸 @ 时
# （http://u:p@ss@h:1_tfoo_v1），旧的「[^…@?]+@」只匹配到首个 @，会残留半截密码
# （输出 http://***@ss@h:1_tfoo_v1，ss 是密码后半段）。改为 [^\s/]+：贪婪吃到 / 或
# 空白为止、再回溯到该段最后一个 @，userinfo 里的裸 @ 一并吞掉；代价是 query 里带
# @ 的畸形串可能多抹几个字符——日志兜底宁多勿漏。scheme 放宽到任意「xxx://」形态
# （httpx 的 all://、socks5:// 等代理 scheme 同样覆盖）。真实连接池 key 走结构分支
# （_mask_proxy_url/urlparse 本就按最后一个 @ 切 userinfo），本规则兜底日志/兜底路径。
_USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/]+@")

def _strip_url_userinfo(text: str) -> str:
    """通用脱敏：抹掉任意 URL 里 scheme://user:pass@ 的凭据段（无需登记表）。

    贪婪到最后一个 @（遇 / 或空白停）：密码含裸 @（user:p@ss@host）也整段抹掉。"""
    return _USERINFO_RE.sub(lambda m: f"{m.group(1)}***@", text)

def _scrub_proxy_secrets(msg: str) -> str:
    """日志兜底：把消息里出现的带凭据代理 URL 替换成脱敏形态（httpx 异常串里可能携带）。
    先替换配置里登记过的代理（输出最干净的无凭据形态），再过一遍通用 userinfo 规则，
    兜住未登记的带凭据 URL（用户实测：未登记的 http://a:pw2@h:1 原样残留）。"""
    for pool in (PROXY_URLS, EXTERNAL_PROXY_URLS):
        for p in pool:
            if p and "@" in p:
                msg = msg.replace(p, _mask_proxy_url(p))
    return _strip_url_userinfo(msg)

# 【P0凭据泄露·补】CLIENT_POOL 的 key 是「完整代理 URL + _t超时 + _v校验」拼接的：一旦
# 发起过经代理的请求，/health 的 connection_pool 会把带账号密码的原始 URL 原样吐出去
#（此前验收没发现是因为测试时连接池里还没有经代理的条目）。输出前先按 key 结构脱敏。
# 【P0凭据泄露·补2】两处同步放宽：①超时匹配支持小数（key 里的 _t7.5 此前匹配不上
# 会掉进兜底分支）；②兜底改用上面的通用 userinfo 剥离，不再依赖代理登记表。
_POOL_KEY_RE = re.compile(r"^(?P<proxy>.+)_t(?P<timeout>\d+(?:\.\d+)?)_v(?P<verify>[01])$")

def _mask_pool_key(key: str) -> str:
    """把连接池 key 里的代理 URL 换成脱敏形态（直连条目不受影响）。
    超时支持小数；结构对不上时按通用规则抹掉 ://user:pass@（未登记的代理同样生效）；
    抹完仍残留「scheme://...@」形态（无法解析的异常 key）时整体打码，宁可不输出也不能泄露。"""
    m = _POOL_KEY_RE.match(key)
    if m:
        return f"{_mask_proxy_url(m.group('proxy'))}_t{m.group('timeout')}_v{m.group('verify')}"
    # 兜底：通用剥离 userinfo（不依赖代理登记表）；仍残留可疑形态则整体打码
    masked = _strip_url_userinfo(key)
    if "://" in masked and "@" in masked and "***@" not in masked:
        return "<masked-pool-key>"
    return masked

# 【P0凭据泄露】启动日志只打脱敏列表（原先整列表打印，带账号密码的代理直接落盘）
logging.info(f"[代理] 国内代理 {len(PROXY_URLS)} 个: {[_mask_proxy_url(p) for p in PROXY_URLS]}")
logging.info(f"[代理] 外网代理 {len(EXTERNAL_PROXY_URLS)} 个: {[_mask_proxy_url(p) for p in EXTERNAL_PROXY_URLS]}")

# 【T23·线上回归】外代池直连兑底开关（默认开）：用户实测（Render 部署）外代 socks5 池
# 整体挂掉（超时 + 自签证书 MITM）后，Twitch/SOOP 解析全军覆没——而部署机本身能直连
# 这些海外 API。旧「外池不直连」假设的是国内部署（直连必被墙，白等超时）；实际部署在
# 海外（Render 等）时直连是唯一活路。默认开启：外代全挂→直连兑底一次，成功后进入
# 冷静期（后续直连优先，不再每个请求白等全池超时）。纯国内部署可设 0 恢复旧行为。
EXTERNAL_DIRECT_FALLBACK = os.getenv("EXTERNAL_DIRECT_FALLBACK", "1") == "1"
if EXTERNAL_DIRECT_FALLBACK:
    logging.info("[代理] 外代池直连兑底已开启（EXTERNAL_DIRECT_FALLBACK=1，外代全挂后直连尝试）")
else:
    logging.info("[代理] 外代池直连兑底已关闭（EXTERNAL_DIRECT_FALLBACK=0，外代全挂直接失败）")

CF_WORKER = os.getenv("CF_WORKER_URL", "").strip()
# 【统一 Cookie】各平台登录 Cookie：前端可通过 /api/parse?cookie=xxx 动态传入，
# 环境变量作为兜底（未传时使用）。两个都没配置时走匿名路径。
SOOP_COOKIE = os.getenv("SOOP_COOKIE", "").strip()
TWITCH_COOKIE = os.getenv("TWITCH_COOKIE", "").strip()
BILI_COOKIE = os.getenv("BILI_COOKIE", "").strip()
HUYA_COOKIE = os.getenv("HUYA_COOKIE", "").strip()
DOUYU_COOKIE = os.getenv("DOUYU_COOKIE", "").strip()
# 虎牙 FLV 默认强制升级 https（防 https 页面下混合内容被浏览器拦截）；
# 个别 CDN 不支持 https 播放时，可设 HUYA_FORCE_HTTPS=0 保留原始协议（E25）
HUYA_FORCE_HTTPS = os.getenv("HUYA_FORCE_HTTPS", "1") == "1"
DOUYIN_COOKIE = os.getenv("DOUYIN_COOKIE", "").strip()

if CF_WORKER:
    logging.info(f"[CF Worker] 已配置: {CF_WORKER}")
else:
    logging.info("[CF Worker] 未配置，海外平台 m3u8 将走外网代理")

# ==================== 全局连接池 ====================
CLIENT_POOL: dict = {}
CLIENT_LOCK = asyncio.Lock()
DEFAULT_TIMEOUT = 15
# 【TLS 校验·2026-10-16 多平台实测后修订】默认回到关闭（VERIFY_TLS 默认 0，恢复旧版行为）：
# 海外代理池（美国家宽 socks5）对 SOOP/Twitch 域名做 TLS 中间盒（自签证书）——开启校验后
# 7/8 台 SOOP、6/8 台 Twitch 直接 CERTIFICATE_VERIFY_FAILED，海外平台解析全线报错；
# 国内池证书干净（60 次浸泡 verify=True 全 200，不受影响）。
# - VERIFY_TLS=0：全局关闭（默认，启动时告警）；=1：全局开启
# - 想开启校验必须同时豁免被中间盒的域名（否则海外全挂）：
#   VERIFY_TLS=1 + INSECURE_TLS_HOSTS=sooplive.com,sooplive.net,sooplivecdn.com,twitch.tv,ttvnw.net,twitchsvc.net
VERIFY_TLS = os.getenv("VERIFY_TLS", "0") == "1"
INSECURE_TLS_HOSTS = [h.strip().lower() for h in os.getenv("INSECURE_TLS_HOSTS", "").split(",") if h.strip()]

if not VERIFY_TLS:
    logging.warning("[安全] VERIFY_TLS=0：已关闭 TLS 证书校验（海外代理池含自签证书中间盒，属本项目预期配置；需校验时设 VERIFY_TLS=1 + INSECURE_TLS_HOSTS 豁免海外域名）")
elif INSECURE_TLS_HOSTS:
    logging.warning(f"[安全] 以下域名例外不校验 TLS 证书: {INSECURE_TLS_HOSTS}")

def _tls_verify_for(url: str) -> bool:
    """按目标域名决定是否校验证书：INSECURE_TLS_HOSTS 及其子域关，其余默认校验。"""
    if not VERIFY_TLS:
        return False
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return True
    return not any(host == h or host.endswith("." + h) for h in INSECURE_TLS_HOSTS)
# HTTP/2 开关：个别代理对 h2 隧道兼容性差（连接失败/挂起）时，可设 HTTP2_ENABLED=0 一键降级 HTTP/1.1；
# 注意 httpx 的 http2=True 必须有 h2 包（requirements.txt 已显式声明，原先靠传递依赖带入）
HTTP2_ENABLED = os.getenv("HTTP2_ENABLED", "1") == "1"

# 【connect 超时·2026-10-16 多平台实测后修订】默认回到 15s（旧版行为；env CONNECT_TIMEOUT 可调）。
# 曾收紧到 5s 防止死代理拖时间，但实测表明：跨境部署到国内代理的连接时长有 1~15s 的毛刺，
# 5s 会把「慢但活」的代理误判为死 → 全池扫荡 + 直连兜底 + 冷静期（180s）的连锁，
# 表现为「打开直播间好久才播放 / 日志刷失败 / 有弹幕没画面」。read/write 仍用整体超时值。
DEFAULT_CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "15"))

async def get_client(proxy=None, timeout=None, verify: bool = True):
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    if isinstance(timeout, (int, float)):
        timeout = httpx.Timeout(float(timeout), connect=DEFAULT_CONNECT_TIMEOUT)
    key = f"{proxy or 'direct'}_t{timeout}_v{int(bool(verify))}"
    entry = CLIENT_POOL.get(key)
    if entry is not None:
        client, _ = entry
        CLIENT_POOL[key] = (client, time.time())
        return client
    async with CLIENT_LOCK:
        entry = CLIENT_POOL.get(key)
        if entry is not None:
            client, _ = entry
            CLIENT_POOL[key] = (client, time.time())
            return client
        client = httpx.AsyncClient(
            timeout=timeout,
            proxy=proxy,
            http2=HTTP2_ENABLED,
            verify=verify,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20
            )
        )
        CLIENT_POOL[key] = (client, time.time())
        return client

STREAM_PROXY_MAP: dict = {}
STREAM_PROXY_MAP_TS: dict = {}
M3U8_CACHE: dict = {}
_SOOP_OFFLINE_COUNT: dict = {}
_DOUYIN_NO_STREAM_LOGGED: dict = {}

_PROXY_HEALTH: dict = {}

def _record_proxy_health(proxy, ok: bool, tag: str = None, error: str = None):
    key = proxy or "直连"
    h = _PROXY_HEALTH.setdefault(key, {
        "success": 0, "fail": 0,
        "last_success_ts": None, "last_fail_ts": None,
        "last_error": None, "by_tag": {}
    })
    if ok:
        h["success"] += 1
        h["last_success_ts"] = time.time()
    else:
        h["fail"] += 1
        h["last_fail_ts"] = time.time()
        h["last_error"] = error
    if tag:
        t = h["by_tag"].setdefault(tag, {"success": 0, "fail": 0})
        t["success" if ok else "fail"] += 1

_STREAM_TAGS = ("SOOP-live", "SOOP-cdn", "SOOP-master", "SOOP-aid",
                 "Twitch-m3u8", "Twitch-token", "PandaTV-play", "PandaTV-master")

def _short_url(url: str, length: int = 60) -> str:
    # 【P0日志泄露】query 只记参数名不记值（原先保留前 20 字符会截到半个鉴权 token）；
    # 参数名足以定位「哪类签名出问题」，具体值排查时再单独查（D21 行为变更）
    path, _, query = url.partition('?')
    if query:
        names = [kv.split('=')[0] for kv in query.split('&') if kv]
        path = f"{path}?{'&'.join(names)}"
    return path if len(path) <= length else path[:length] + "…"

_PROXY_RR_LOCK = asyncio.Lock()
_PROXY_RR_CYCLES: dict = {}

def _get_rr_cycle(proxy_pool):
    # 【L10·审计修复】原以 id(pool) 为键：对象销毁后 id 可能被复用、且条目永不清理。
    # 改为内容元组键（池成员为 str/None，可哈希且数量有界），语义不变、无泄漏面。
    pool_key = tuple(proxy_pool)
    if pool_key not in _PROXY_RR_CYCLES:
        _PROXY_RR_CYCLES[pool_key] = itertools.cycle(range(len(proxy_pool)))
    return _PROXY_RR_CYCLES[pool_key]

async def get_fixed_proxy_list(proxy_pool):
    if not proxy_pool:
        return [None]
    if len(proxy_pool) == 1 and proxy_pool[0] is None:
        return [None]
    n = len(proxy_pool)
    async with _PROXY_RR_LOCK:
        cycle = _get_rr_cycle(proxy_pool)
        start = next(cycle)
    return [proxy_pool[(start + i) % n] for i in range(n)]

# 代理池整体不可达时的冷静期：期间请求先试直连，避免每个解析都等完全部代理超时。
# 【P1：冷静期分池】此前是单一全局量——内池（PROXY_URLS，国内平台）与外池
# （EXTERNAL_PROXY_URLS，海外平台）共用：外代池连不通（部署环境无外网代理时常态）
# 会误让国内请求也直连优先（国内代理明明可用）；反之亦然。改为按池分桶。
# 【健壮性】600s 太钝：池劣化（而非全死）场景下用户要 10 分钟才恢复代理试用；
# 收紧到 180s（env 可调），期间单请求最多吃一次全池扫荡的代价（connect 超时见
# DEFAULT_CONNECT_TIMEOUT，默认 15s）。
_PROXY_POOL_DEAD_UNTIL: dict[str, float] = {}
_PROXY_POOL_DEAD_SECONDS = float(os.getenv("PROXY_POOL_DEAD_SECONDS", "180"))


def _proxy_pool_key(proxy_list: list) -> str:
    """冷静期分桶键：按列表成员归属判定「内池/外池/混合/纯直连」。
    TS 粘性代理（STREAM_PROXY_MAP 固定单代理）与 get_fixed_proxy_list 轮转子集
    都能正确归桶（子集成员 ∈ 原池即归原池）；两池配了同一个代理时归内桶（保守）。"""
    real = [p for p in proxy_list if p is not None]
    if not real:
        return "direct"
    if all(p in PROXY_URLS for p in real):
        return "internal"
    if all(p in EXTERNAL_PROXY_URLS for p in real):
        return "external"
    return "misc"


async def _sequential_request(method, url, proxy_list, fail_log_prefix, timeout, log_tag, **kwargs):
    pool_key = _proxy_pool_key(proxy_list)
    has_real_proxy = any(p is not None for p in proxy_list)
    attempts: list = list(proxy_list)
    direct_tried = attempts and attempts[0] is None
    # 【P1二轮→T23修订：外池直连兑底可配（默认开）】旧版外池默认不直连（假设国内部署
    # 直连必被墙）；但用户实际部署在 Render（美国机房）——外代 socks5 池整体挂掉时
    # （实测：超时 + 自签证书 MITM），直连是唯一活路，关闭它直接导致 Twitch/SOOP 全
    # 解析失败。改为 EXTERNAL_DIRECT_FALLBACK（默认 1）：海外部署外代全挂→直连兑底；
    # 纯国内部署设 0 恢复「不白等直连超时」的旧行为。冷静期语义不变。
    allow_direct = pool_key != "external" or EXTERNAL_DIRECT_FALLBACK
    if has_real_proxy and allow_direct and not direct_tried and time.time() < _PROXY_POOL_DEAD_UNTIL.get(pool_key, 0.0):
        # 冷静期内直连优先（部署环境连不通代理时，避免每个请求都等完全部代理超时）
        attempts = [None] + attempts
        direct_tried = True
    last_error = None
    last_index = len(attempts) - 1
    for i, proxy in enumerate(attempts):
        try:
            client = await get_client(proxy, timeout, verify=_tls_verify_for(url))
            resp = await client.request(method, url, **kwargs)
            _record_proxy_health(proxy, True, tag=log_tag)
            if log_tag:
                logging.info(f"[{log_tag}] {_mask_proxy_url(proxy)} → HTTP {resp.status_code} {_short_url(url)}")
            return resp
        except Exception as e:
            _record_proxy_health(proxy, False, tag=log_tag, error=f"{type(e).__name__}: {e}")
            last_error = e
            logging.warning(_scrub_proxy_secrets(f"{fail_log_prefix}{f'[{log_tag}]' if log_tag else ''} {_mask_proxy_url(proxy)} 失败 [{type(e).__name__}]: {e}"))
            if i != last_index:
                await asyncio.sleep(0.5)
    if has_real_proxy:
        if not direct_tried and allow_direct:
            # 全部代理失败后直连兜底一次（如代理池在当前部署环境不可达）
            try:
                client = await get_client(None, timeout, verify=_tls_verify_for(url))
                resp = await client.request(method, url, **kwargs)
                _record_proxy_health(None, True, tag=log_tag)
                # 【P1二轮：冷静期唯一设置点】代理全挂 + 直连成功才设置——冷静期
                # 的语义就是「代理死了、直连可用，后续直连优先」。
                _PROXY_POOL_DEAD_UNTIL[pool_key] = time.time() + _PROXY_POOL_DEAD_SECONDS
                logging.info(f"{fail_log_prefix}{f'[{log_tag}] ' if log_tag else ''}代理全部失败，直连兜底成功 → HTTP {resp.status_code} {_short_url(url)}")
                return resp
            except Exception as e:
                last_error = last_error or e
                logging.warning(_scrub_proxy_secrets(f"{fail_log_prefix}{f'[{log_tag}]' if log_tag else ''} 直连兜底失败 [{type(e).__name__}]: {e}"))
                # 【P1二轮：冷静期误触发】直连也失败（整个出口网络不可用）时不再
                # 设置冷静期：设了只会让后续请求跳过代理、白等一次直连超时再全链
                # 失败；外池直连被禁（EXTERNAL_DIRECT_FALLBACK=0）未尝试直连同样不设。
        # 非直连成功路径（直连失败 / 外池直连被禁 / 直连本就在尝试序列里且失败）
        # ——都不设冷静期，直接抛出失败。
    raise last_error or Exception("所有代理均失败")

async def request_with_retry(method, url, **kwargs):
    timeout = kwargs.pop("timeout", 15)
    log_tag = kwargs.pop("log_tag", None)
    return await _sequential_request(method, url, PROXY_URLS, "[请求重试]", timeout, log_tag, **kwargs)

async def request_with_proxy_group(method, url, proxy_list, **kwargs):
    timeout = kwargs.pop("timeout", 15)
    shuffle_proxy = kwargs.pop("shuffle_proxy", False)
    log_tag = kwargs.pop("log_tag", None)
    targets = proxy_list[:]
    if shuffle_proxy and targets:
        random.shuffle(targets)
    return await _sequential_request(method, url, targets, "[分组请求]", timeout, log_tag, **kwargs)


async def stream_request_with_proxy_group(method, url, proxy_list, headers=None, content=None,
                                           shuffle_proxy=False, timeout=15, log_tag=None):
    pool_key = _proxy_pool_key(proxy_list)
    proxies = proxy_list[:] if proxy_list else [None]
    if shuffle_proxy and proxies:
        random.shuffle(proxies)
    has_real_proxy = any(p is not None for p in proxies)
    direct_tried = proxies[0] is None
    # 【P1二轮→T23修订】同 _sequential_request：外池直连兑底受 EXTERNAL_DIRECT_FALLBACK
    # 控制（默认开，用户 Render 部署外代全挂时靠直连恢复海外解析；国内部署设 0）。
    allow_direct = pool_key != "external" or EXTERNAL_DIRECT_FALLBACK
    if has_real_proxy and allow_direct and not direct_tried and time.time() < _PROXY_POOL_DEAD_UNTIL.get(pool_key, 0.0):
        proxies = [None] + proxies
        direct_tried = True
    last_error = None
    last_index = len(proxies) - 1
    for i, proxy in enumerate(proxies):
        try:
            client = await get_client(proxy, timeout, verify=_tls_verify_for(url))
            req = client.build_request(method, url, headers=headers, content=content)
            resp = await client.send(req, stream=True)
            _record_proxy_health(proxy, True, tag=log_tag)
            if log_tag:
                logging.info(f"[{log_tag}] {_mask_proxy_url(proxy)} → HTTP {resp.status_code} {_short_url(url)}（流式）")
            return resp
        except Exception as e:
            _record_proxy_health(proxy, False, tag=log_tag, error=f"{type(e).__name__}: {e}")
            last_error = e
            logging.warning(_scrub_proxy_secrets(f"[流式代理]{f'[{log_tag}]' if log_tag else ''} {_mask_proxy_url(proxy)} 失败 [{type(e).__name__}]: {e}"))
            if i != last_index:
                await asyncio.sleep(0.5)
    if has_real_proxy:
        if not direct_tried and allow_direct:
            try:
                client = await get_client(None, timeout, verify=_tls_verify_for(url))
                req = client.build_request(method, url, headers=headers, content=content)
                resp = await client.send(req, stream=True)
                _record_proxy_health(None, True, tag=log_tag)
                # 【P1二轮：冷静期唯一设置点】同 _sequential_request：代理全挂+
                # 直连成功才设；直连也失败/外池未尝试直连都不设。
                _PROXY_POOL_DEAD_UNTIL[pool_key] = time.time() + _PROXY_POOL_DEAD_SECONDS
                logging.info(f"{'[' + log_tag + '] ' if log_tag else ''}代理全部失败，直连兜底成功（流式）: {_short_url(url)}")
                return resp
            except Exception as e:
                last_error = last_error or e
                logging.warning(_scrub_proxy_secrets(f"[流式代理]{f'[{log_tag}]' if log_tag else ''} 直连兜底失败 [{type(e).__name__}]: {e}"))
    raise last_error or Exception("所有代理均失败（流式）")


# ==================== 平台域名白名单（P0-1：分派/分流判定统一收口） ====================
# 【为什么】此前 main.py 里四处域名判定各自为政，且全是「子串 in url」：
#   ① _parse_dispatch：if "douyin.com" in url —— https://douyin.com.evil.com/ 或
#      https://evil.com/?q=douyin.com 都能混进分派，而 parse_douyin 对传入 URL 有
#      两处裸抓取（DouyinLiveStream.fetch_web_stream_data / request_with_retry 直拉页面），
#      任意 URL 会被后端直接 fetch —— SSRF；
#   ② api_proxy use_external：any(d in url) —— 域名出现在路径/查询串（如
#      /api/redirect?target=twitch.tv）也会误切外代池；
#   ③ api_follows_batch 的 _FOREIGN_MARKERS：同款子串分池；
#   ④ api_proxy m3u8 分流（_is_foreign_stream / eff_referer 兜底）："twitch" in url
#      等宽子串，国内 CDN 路径里含 twitch 会被误判成海外流走 CF Worker。
# 统一为「urlparse 解析 hostname → 去掉 www. 前缀 → 全等/点分后缀匹配」，
# 一张平台表 + 一组外域族表，四处共用；语义与 browse_engine.resolve_site 的
# _DOMAIN_TO_SITE 一致（那张表挂在 browse 侧，不并表以免反向依赖）。
# 注意：分派要求 URL 带 scheme（urlparse 对裸域名解析不出 hostname）；
# 前端 search.vue 调 /api/parse 前已统一补 https://（parseAndGo 的 full），
# browse 路由传入的全是 make_room_url 拼好的规范 URL，两条入口都不受影响。

PLATFORM_SITES: dict[str, str] = {
    "huya.com": "huya",
    "douyu.com": "douyu",
    "bilibili.com": "bilibili",
    "douyin.com": "douyin",
    "twitch.tv": "twitch",
    "sooplive.com": "soop",
    "pandalive.co.kr": "pandatv",
}

# 外域链路（走 EXTERNAL_PROXY_URLS / CF Worker）：海外平台域 + 流 CDN 域
EXTERNAL_STREAM_DOMAINS: tuple[str, ...] = (
    "twitch.tv", "ttvnw.net", "twitchsvc.net",
    "sooplive.com", "sooplive.net", "sooplivecdn.com",
    "pandalive.co.kr", "pandalivecdn.com", "live-video.net",
)
# 外域 referer（referer 指向这些站说明播放器正在取海外流）
EXTERNAL_STREAM_REFERERS: tuple[str, ...] = (
    "twitch.tv", "player.twitch.tv", "sooplive.com", "pandalive.co.kr",
)
# 关注批量解析的海外/国内信号量分池（平台域级别，与 use_external 的域名族对齐）
FOREIGN_ROOM_DOMAINS: tuple[str, ...] = ("twitch.tv", "sooplive.com", "pandalive.co.kr")


def _host_of(value: str) -> str:
    """取 URL 的 hostname（小写）；空串/解析失败/无 scheme 时返回 ""。"""
    if not value:
        return ""
    try:
        return (urlparse(value).hostname or "").lower()
    except ValueError:
        # urlparse 对畸形 URL（如 http://[invalid）抛 ValueError——按未知主机处理
        return ""


def _host_matches(host: str, domain: str) -> bool:
    """host == domain 或 host 是 domain 的子域；douyin.com.evil.com ≠ douyin.com 的子域。"""
    if not host:
        return False
    return host == domain or host.endswith("." + domain)


def _host_in(value: str, domains: tuple[str, ...]) -> bool:
    """value 的 hostname 命中 domains 中任一域（全等或点分后缀）。"""
    return any(_host_matches(_host_of(value), d) for d in domains)


def _site_of_url(url: str) -> str:
    """按主机名识别平台（huya/douyu/bilibili/douyin/twitch/soop/pandatv），未命中返回 ""。"""
    host = _host_of(url)
    if host.startswith("www."):
        host = host[4:]
    for domain, site in PLATFORM_SITES.items():
        if _host_matches(host, domain):
            return site
    return ""


def _use_external_proxies(url: str, referer: str = "") -> bool:
    """api_proxy 内外代理池切换：URL 或 referer 的【主机名】命中外域族才切外代池。"""
    return _host_in(url, EXTERNAL_STREAM_DOMAINS) or _host_in(referer, EXTERNAL_STREAM_REFERERS)


# ------------------ 代理接口 -----------------
@app.api_route("/api/proxy", methods=["GET", "POST"])
@limiter.limit("60/minute")
async def api_proxy(request: Request, url: str = Query(...), referer: str = Query(""), ua: str = Query("")):
    # P0-7/8：Cookie 改 x-platform-cookie 头（旧 ?cookie= 查询串会落日志；
    # 现有前端两处调用均不传 cookie，无兼容包袱）
    cookie = request.headers.get("x-platform-cookie", "")
    ALLOWED = [
        ".douyu.com", ".huya.com", ".bilibili.com", ".bilivideo.com", ".douyucdn.cn",
        ".douyin.com", ".live.bilibili.com", ".twitch.tv", ".ttvnw.net",
        ".sooplive.com", ".sooplive.net", ".sooplivecdn.com",
        ".pandalive.co.kr", ".live-video.net", ".pandalivecdn.com",
    ]

    def is_allowed_domain(url: str) -> bool:
        hostname = urlparse(url).hostname
        if not hostname:
            return False
        for domain in ALLOWED:
            clean = domain.lstrip('.')
            if hostname == clean or hostname.endswith('.' + clean):
                return True
        return False

    if not is_allowed_domain(url):
        raise HTTPException(403, "domain not allowed")

    body = await request.body() if request.method == "POST" else None
    headers = {"User-Agent": ua or UA, "Referer": referer or ""}
    if cookie:
        headers["Cookie"] = cookie
    if request.method == "POST":
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    # P0-1：内外代理池判定改走统一 hostname 白名单（原 any(d in url) 子串——
    # 域名出现在路径/查询串也会误切外代池，如 /api/redirect?target=twitch.tv）
    use_external = _use_external_proxies(url, referer)
    proxy_list = EXTERNAL_PROXY_URLS if use_external else PROXY_URLS
    is_ts = url.lower().split("?")[0].endswith(".ts")

    if is_ts:
        stream_key = hashlib.md5(url.encode()).hexdigest()[:16]
        if stream_key not in STREAM_PROXY_MAP:
            if proxy_list and proxy_list[0] is not None:
                STREAM_PROXY_MAP[stream_key] = random.choice(proxy_list)
            else:
                STREAM_PROXY_MAP[stream_key] = None
            STREAM_PROXY_MAP_TS[stream_key] = time.time()
        else:
            STREAM_PROXY_MAP_TS[stream_key] = time.time()
        fixed_proxy = STREAM_PROXY_MAP[stream_key]
        proxies_to_use = [fixed_proxy] if fixed_proxy else proxy_list
        shuffle_proxy = False
    else:
        proxies_to_use = proxy_list
        shuffle_proxy = True

    resp = await stream_request_with_proxy_group(
        request.method, url,
        proxy_list=proxies_to_use,
        headers=headers, content=body,
        shuffle_proxy=shuffle_proxy,
    )
    content_type = resp.headers.get("content-type", "")
    is_m3u8 = "mpegurl" in content_type.lower() or url.split("?")[0].endswith(".m3u8")

    if is_m3u8:
        try:
            await resp.aread()
        finally:
            await resp.aclose()
        base_url = url.rsplit("/", 1)[0] + "/"
        parsed_cdn = urlparse(url)
        cdn_origin = f"{parsed_cdn.scheme}://{parsed_cdn.netloc}"
        proxy_base = str(request.base_url).rstrip("/") + "/api/proxy"
        # P0-1：海外流判定改 hostname 白名单（原 any(d in url) 子串——"twitch" 出现在
        # 国内 CDN 路径里也会误判，导致国内流被改走 CF Worker + 海外 referer）
        _is_foreign_stream = _host_in(url, EXTERNAL_STREAM_DOMAINS)
        if CF_WORKER and _is_foreign_stream:
            proxy_base = CF_WORKER.rstrip("/")
        if referer:
            eff_referer = referer
        elif _host_in(url, ("pandalive.co.kr", "pandalivecdn.com", "live-video.net")):
            eff_referer = "https://www.pandalive.co.kr/"
        elif _host_in(url, ("twitch.tv", "ttvnw.net", "twitchsvc.net")):
            eff_referer = "https://player.twitch.tv"
        else:
            eff_referer = "https://play.sooplive.com"

        text = resp.text
        lines = text.splitlines()
        rewritten = []

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#EXT-X-MEDIA:") or stripped.startswith("#EXT-X-MAP:") or stripped.startswith("#EXT-X-KEY:"):
                def replace_uri(match):
                    uri = match.group(1)
                    # data:/skd:/urn: 等自定义 scheme（典型：内联 AES-128 key 的 data: URI）
                    # 不能当相对路径拼进 base_url（会拼出坏地址导致解密失败），原样保留（E24）
                    if re.match(r'^[a-zA-Z][a-zA-Z0-9+.\-]*:', uri) and not uri.startswith(("http://", "https://")):
                        return match.group(0)
                    if uri.startswith("http://") or uri.startswith("https://"):
                        abs_uri = uri
                    elif uri.startswith("/"):
                        abs_uri = cdn_origin + uri
                    else:
                        abs_uri = base_url + uri
                    proxied = f"{proxy_base}?url={quote(abs_uri, safe='')}&referer={quote(eff_referer, safe='')}"
                    return f'URI="{proxied}"'
                new_line = re.sub(r'URI="([^"]+)"', replace_uri, line)
                rewritten.append(new_line)
                continue

            if stripped and not stripped.startswith("#"):
                if stripped.startswith("http://") or stripped.startswith("https://"):
                    abs_url = stripped
                elif stripped.startswith("/"):
                    abs_url = cdn_origin + stripped
                else:
                    abs_url = base_url + stripped
                proxied = f"{proxy_base}?url={quote(abs_url, safe='')}&referer={quote(eff_referer, safe='')}"
                rewritten.append(proxied)
            else:
                rewritten.append(line)

        body_out = "\n".join(rewritten).encode("utf-8")
        out_headers = {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/vnd.apple.mpegurl",
        }
        return Response(content=body_out, status_code=resp.status_code, headers=out_headers)

    if is_ts:
        async def _stream_and_close():
            total = 0
            try:
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > MAX_TS_SIZE:
                        # P1（TS 截断改断连）：MPEG-TS 是流式容器，截断的切片对播放器
                        # 「看起来是完整文件」——花屏/跳帧等静默劣化（数据正确性问题）。
                        # 改为超限时直接断开响应（不发终止块），播放器收到网络错误走
                        # 重试/切线路，不再把半截坏数据当正常切片吞下去。
                        logging.warning(f"[代理] TS 切片超过 {MAX_TS_SIZE} 字节，断开连接拒绝交付截断数据: {_short_url(url)}")
                        raise _TsSegmentTooLarge(f"ts segment over limit: {total} > {MAX_TS_SIZE}")
                    yield chunk
            finally:
                try:
                    await resp.aclose()
                except Exception:
                    pass

        return StreamingResponse(
            _stream_and_close(),
            status_code=resp.status_code,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Content-Type": content_type or "video/mp2t"
            }
        )

    async def _stream_and_close_fallback():
        # 兜底分支维持截断（不学 TS 改断连）：这里的响应是 JSON/图片等，截断后客户端
        # parse/解码会明确报错（可检错），不存在 TS 那种「看似完整实为坏数据」的静默劣化。
        total = 0
        truncated = False
        try:
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > MAX_TS_SIZE:
                    truncated = True
                    logging.warning(f"[代理] 响应体超过 {MAX_TS_SIZE} 字节，提前截断: {_short_url(url)}")
                    break
                yield chunk
        finally:
            try:
                await resp.aclose()
            except Exception:
                pass
            if truncated:
                logging.info(f"[代理] 兜底分支截断完成，共转发 {total} 字节")

    out_headers = {"Access-Control-Allow-Origin": "*", "Content-Type": content_type or "application/json"}
    return StreamingResponse(_stream_and_close_fallback(), status_code=resp.status_code, headers=out_headers)


def parse_multivariant_m3u8(text, base_url, cdn_prefix):
    streams = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        name = "Source"
        res_match = re.search(r'RESOLUTION=(\d+x\d+)', line)
        if res_match:
            name = res_match.group(1).split('x')[1] + 'p'
        else:
            bw_match = re.search(r'BANDWIDTH=(\d+)', line)
            if bw_match:
                kbps = int(int(bw_match.group(1)) / 1000)
                name = f"{kbps}k"
        if i + 1 < len(lines):
            sub_url = lines[i + 1].strip()
            if not sub_url:
                continue
            if not sub_url.startswith("http"):
                sub_url = urljoin(base_url, sub_url)
            streams.append({"cdn": f"{cdn_prefix}-{name}", "url": sub_url, "type": "m3u8"})

    if streams:
        def _quality_key(s):
            n = s['cdn'].replace(f'{cdn_prefix}-', '')
            if 'Source' in n:
                return 999999
            num = re.search(r'\d+', n)
            return float(num.group()) if num else 0
        streams.sort(key=_quality_key, reverse=True)
    return streams

# ==================== 虎牙 ====================
def _extract_huya_danmaku_params(live):
    # 原实现 int(非数字串) 抛 ValueError 被外层 except 静默吞掉 → 弹幕参数丢失且无任何日志（A2）。
    # 改为安全解析：非数字降级为 0 并留警告，而不是整个返回 {}。
    try:
        profile_info = live.get("profileInfo") or {}
        live_data = live.get("liveData") or {}
        raw_uid = (
            profile_info.get("lUid") or profile_info.get("uid") or
            live_data.get("lUid") or live_data.get("uid") or
            live_data.get("lYyid") or profile_info.get("lYyid") or 0
        )
        try:
            uid = int(str(raw_uid).strip() or "0")
        except (TypeError, ValueError):
            logging.warning(f"[虎牙] 弹幕 uid 非数字，已降级为 0: {raw_uid!r}")
            uid = 0
        return {"platform": "huya", "uid": uid, "ayyuid": uid}
    except Exception:
        return {}


def huya_build_anticode(raw_anti, stream_name):
    anti = raw_anti.replace("&amp;", "&")
    params = dict(p.split("=", 1) for p in anti.split("&") if "=" in p)
    fm = params.get("fm", "")
    ws_time = params.get("wsTime", "")
    if not fm or not ws_time:
        return anti
    try:
        fm_dec = base64.b64decode(fm.replace("%2B", "+").replace("%2F", "/").replace("%3D", "=") + "==").decode()
    except Exception:
        try:
            fm_dec = base64.b64decode(unquote(fm) + "==").decode()
        except Exception:
            return anti
    p = fm_dec.split("_")[0]
    seqid = str(int(time.time() * 10000 + random.random() * 10000))
    ws_secret = hashlib.md5(f"{p}_0_{stream_name}_{seqid}_{ws_time}".encode()).hexdigest()
    params["wsSecret"] = ws_secret
    params["seqid"] = seqid
    params["u"] = "0"
    return "&".join(f"{k}={v}" for k, v in params.items())

async def parse_huya(url, cookie: str = ""):
    """
    【统一 Cookie】cookie 为空时行为与原先完全一致（匿名路径）；
    有 cookie 时优先使用用户 Cookie（可能包含登录身份），可拿到部分房间的高码率画质。
    """
    try:
        room_id = url.rstrip("/").split("/")[-1].split("?")[0]
        CDN_NAMES = {"AL": "阿里云", "TX": "腾讯云", "HW": "华为云", "WS": "网宿", "BD": "百度云"}
        CDN_ORDER = {"TX": 0, "AL": 1, "HW": 2, "WS": 3, "BD": 4}

        eff_cookie = cookie or HUYA_COOKIE
        base_headers = {"User-Agent": UA, "Referer": "https://www.huya.com/"}
        if eff_cookie:
            base_headers["Cookie"] = eff_cookie

        resp = await request_with_retry("GET", f"https://mp.huya.com/cache.php?m=Live&do=profileRoom&roomid={room_id}",
            headers=base_headers)
        data = resp.json()
        if data.get("status") != 200:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        live = data["data"]

        profile_info = live.get("profileInfo", {})
        live_data = live.get("liveData", {})

        anchor_name = (
            profile_info.get("nick") or live_data.get("nick") or
            profile_info.get("sNick") or live_data.get("sNick") or ""
        )
        if not anchor_name:
            try:
                mob_headers = {"User-Agent": MOBILE_UA, "Referer": "https://www.huya.com/"}
                if eff_cookie:
                    mob_headers["Cookie"] = eff_cookie
                mob_resp = await request_with_retry("GET", f"https://m.huya.com/{room_id}", headers=mob_headers)
                mob_html = mob_resp.text
                m = re.search(r'"nick":"([^"]+)"', mob_html) or re.search(r'<title>([^_<]+)', mob_html)
                if m:
                    anchor_name = m.group(1).strip()
                else:
                    logging.warning(f"[虎牙] {room_id} 网页抓取未匹配到昵称")
            except Exception:
                logging.warning(f"[虎牙] {room_id} 网页抓取请求失败")
        anchor_name = anchor_name or "虎牙主播"

        avatar = (
            profile_info.get("avatar180") or profile_info.get("sAvatar180") or
            profile_info.get("sAvatar") or profile_info.get("avatar") or
            live_data.get("avatar180") or live_data.get("sAvatar180") or
            live_data.get("sAvatar") or live_data.get("avatar") or ""
        )

        if live.get("realLiveStatus") != "ON":
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}

        bitrate_list = live_data.get("bitRateInfo") or live.get("bitRateInfo") or live.get("liveBitRateInfo") or []
        if isinstance(bitrate_list, str):
            try:
                bitrate_list = json.loads(bitrate_list)
            except Exception:
                bitrate_list = []
        qualities = []
        seen_bitrate = set()
        for item in bitrate_list or []:
            if not isinstance(item, dict):
                continue
            raw_b = item.get("bitRate")
            if raw_b is None:
                raw_b = item.get("iBitRate")
            try:
                b = int(raw_b or 0)
            except (TypeError, ValueError):
                b = 0
            name = (item.get("name") or item.get("sDisplayName") or "").strip()
            if not name or b in seen_bitrate:
                continue
            seen_bitrate.add(b)
            qualities.append((b, name))
        qualities.sort(key=lambda x: (0 if x[0] == 0 else 1, -x[0]))
        if not qualities:
            qualities = [(0, "原画"), (4000, "蓝光"), (2000, "超清"), (1000, "高清"), (500, "标清")]

        cdn_list = live.get("stream", {}).get("baseSteamInfoList", [])
        if not cdn_list:
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}
        cdn_list.sort(key=lambda s: CDN_ORDER.get(s.get("sCdnType", "ZZ"), 9))

        streams = []
        seen_urls = set()

        for cdn in cdn_list:
            flv_url = cdn.get("sFlvUrl", "")
            base_stream_name = cdn.get("sStreamName", "")
            anti_code = cdn.get("sFlvAntiCode", "")
            suffix = cdn.get("sFlvUrlSuffix", "flv")
            line_index = cdn.get("iLineIndex", "")
            cdn_type = cdn.get("sCdnType", "")
            cdn_name = CDN_NAMES.get(cdn_type, cdn_type or "线路")
            if line_index not in ("", None):
                line_label = f"{cdn_name}线路{line_index}"
            else:
                line_label = cdn_name

            if not (flv_url and base_stream_name and anti_code):
                continue

            for bitrate, quality_name in qualities:
                stream_name = base_stream_name if bitrate == 0 else f"{base_stream_name}_{bitrate}"
                built = huya_build_anticode(anti_code, stream_name)
                full_url = f"{flv_url}/{stream_name}.{suffix}?{built}"
                if HUYA_FORCE_HTTPS:
                    full_url = full_url.replace("http://", "https://")

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                streams.append({
                    "cdn": f"{line_label}-{quality_name}",
                    "line": line_label,
                    "quality": quality_name,
                    "url": full_url,
                    "type": "flv"
                })

        if not streams:
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}

        danmaku = _extract_huya_danmaku_params(live)
        return {"streams": streams, "title": anchor_name, "avatar": avatar, "danmaku": danmaku, "isLive": True}
    except Exception:
        logging.exception("[虎牙] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== 斗鱼 ====================
DOUYU_FIXED_DID = "10000000000000000000000000001501"

async def parse_douyu(url, cookie: str = ""):
    """
    【统一 Cookie】cookie 为空时行为与原先完全一致（用固定 did 走匿名路径）；
    有 cookie 时优先使用用户 Cookie（携带登录身份），可解锁 2K/4K 60帧等高画质。
    """
    try:
        room_id = url.rstrip("/").split("/")[-1].split("?")[0]

        did = DOUYU_FIXED_DID
        # 【统一 Cookie】用户 Cookie 优先；未提供则回退到设备 Cookie（匿名）
        if cookie:
            effective_cookie = cookie
        else:
            effective_cookie = f"dy_did={did}; acf_did={did}"
        hdrs = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "origin": "https://www.douyu.com",
            "referer": f"https://www.douyu.com/{room_id}",
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.43",
            "cookie": effective_cookie,
        }

        info_resp = await request_with_retry("GET", f"https://www.douyu.com/betard/{room_id}", headers=hdrs)
        info = info_resp.json()
        room = info.get("room")
        if not room:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}

        name = "斗鱼主播"
        avatar = ""

        try:
            open_api = f"https://open.douyucdn.cn/api/RoomApi/room/{room_id}"
            open_resp = await request_with_retry("GET", open_api, headers=hdrs)
            if open_resp.status_code == 200:
                open_data = open_resp.json()
                if open_data.get("error") == 0:
                    d = open_data.get("data", {})
                    name = d.get("owner_name") or name
                    avatar = d.get("avatar_big") or d.get("avatar_middle") or d.get("avatar") or ""
        except Exception:
            pass

        if not avatar or name == "斗鱼主播":
            name = room.get("nickname") or name
            owner = room.get("owner", {})
            avatar_obj = owner.get("avatar", {})
            if isinstance(avatar_obj, dict):
                avatar = avatar_obj.get("big") or avatar_obj.get("middle") or avatar_obj.get("small") or avatar
            else:
                avatar = room.get("avatar") or room.get("room_pic") or avatar

        if avatar and avatar.startswith("//"):
            avatar = "https:" + avatar

        if room.get("show_status") != 1 or room.get("videoLoop") == 1:
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}

        real_id = str(room["room_id"])
        enc_resp = await request_with_retry("GET", f"https://www.douyu.com/wgapi/livenc/liveweb/websec/getEncryption?did={did}",
                                            headers=hdrs)
        enc_data = enc_resp.json()
        if enc_data.get("error") != 0:
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}
        white = enc_data["data"]
        ts = int(time.time())
        secret = white['rand_str']
        for _ in range(white['enc_time']):
            secret = hashlib.md5((secret + white['key']).encode()).hexdigest()
        salt = "" if white.get('is_special', 0) == 1 else f"{real_id}{ts}"
        auth = hashlib.md5((secret + white['key'] + salt).encode()).hexdigest()

        base_params = {
            'enc_data': white['enc_data'],
            'tt': str(ts),
            'did': did,
            'auth': auth,
            'ver': 'Douyu_new',
            'hevc': '1',
            'fa': '0',
            'ive': '0',
        }

        probe_params = dict(base_params, rate="-1", cdn="hw-h5")
        try:
            await asyncio.sleep(random.uniform(0, 0.15))
            probe = await request_with_retry("POST",
                f"https://www.douyu.com/lapi/live/getH5PlayV1/{real_id}",
                headers=hdrs, data=probe_params, timeout=10)
            probe = probe.json()
        except Exception as e:
            logging.warning(f"[斗鱼] 线路/画质表探测失败: {e}")
            probe = {}
        if probe.get("error") != 0 or not isinstance(probe.get("data"), dict):
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}
        pdata = probe["data"]
        logging.info(f"[斗鱼] room={room_id} 登录={'是' if cookie else '否'} cdnsWithName={pdata.get('cdnsWithName')!r}")
        logging.info(f"[斗鱼] room={room_id} multirates={pdata.get('multirates')!r}")

        lines, seen_cdn = [], set()
        for entry in pdata.get("cdnsWithName") or []:
            cdn_code = str(entry.get("cdn") or "").strip()
            if not cdn_code or cdn_code in seen_cdn:
                continue
            seen_cdn.add(cdn_code)
            lines.append({"name": (entry.get("name") or "").strip() or cdn_code, "cdn": cdn_code})
        if not lines:
            lines = [{"name": "主线路", "cdn": "hw-h5"}]

        rates, seen_rate = [], set()
        for entry in pdata.get("multirates") or []:
            try:
                rv = int(entry.get("rate"))
            except (TypeError, ValueError):
                continue
            if rv in seen_rate:
                continue
            seen_rate.add(rv)
            rates.append({"name": (entry.get("name") or "").strip() or f"画质{rv}",
                          "rate": rv, "bit": int(entry.get("bit") or 0)})
        if not rates:
            rates = [{"name": "原画", "rate": 0, "bit": 0}]
        name_by_bit = {r_["bit"]: r_["name"] for r_ in rates if r_["bit"]}

        async def fetch_combo(line_name, cdn_code, rate_info):
            params = base_params.copy()
            params["rate"] = str(rate_info["rate"])
            params["cdn"] = cdn_code
            try:
                await asyncio.sleep(random.uniform(0, 0.15))
                r = await request_with_retry("POST",
                    f"https://www.douyu.com/lapi/live/getH5PlayV1/{real_id}",
                    headers=hdrs, data=params, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    if d.get("error") == 0:
                        info = d["data"]
                        u = f"{info['rtmp_url']}/{info['rtmp_live']}"
                        # 浏览器 FLV 播放器不支持 rtmp://；斗鱼 H5 接口的 rtmp 与 http 同源，替换协议即可在网页播放
                        u = u.replace("rtmp://", "http://")
                        m_suffix = re.search(r"_([0-9]{3,6})\.flv", u)
                        actual_suffix = m_suffix.group(1) if m_suffix else "无后缀(原画)"
                        logging.info(f"[斗鱼] {line_name} 请求 rate={rate_info['rate']}({rate_info['name']}) → 实际后缀={actual_suffix}")
                        return line_name, rate_info, u
                    logging.warning(f"[斗鱼] {line_name} 画质 rate={rate_info['rate']} 接口返回错误: {d.get('error')} {d.get('msg','')}")
                else:
                    logging.warning(f"[斗鱼] {line_name} 画质 rate={rate_info['rate']} HTTP {r.status_code}")
            except Exception as e:
                logging.warning(f"[斗鱼] {line_name} 画质 rate={rate_info['rate']} 请求异常: {e}")
            return line_name, rate_info, None

        combos = [(l_["name"], l_["cdn"], r_) for l_ in lines for r_ in rates]
        results = []
        for chunk in [combos[i:i + 8] for i in range(0, len(combos), 8)]:
            results += await asyncio.gather(*(fetch_combo(*cb) for cb in chunk))

        streams = []
        seen_urls = set()
        seen_pair = set()
        for line_name, rate_info, flv_url in results:
            if not flv_url or flv_url in seen_urls:
                continue
            seen_urls.add(flv_url)
            m = re.search(r"_([0-9]{3,6})\.flv", flv_url)
            actual_bit = int(m.group(1)) if m else rate_info["bit"]
            key = (line_name, actual_bit)
            if key in seen_pair:
                continue
            seen_pair.add(key)
            if actual_bit and rate_info["bit"] and actual_bit != rate_info["bit"]:
                q_name = f"{rate_info['name']}→{actual_bit//1000}k"
            else:
                q_name = name_by_bit.get(actual_bit) or rate_info["name"]
            streams.append({
                "cdn": f"{line_name}-{q_name}",
                "line": line_name,
                "quality": q_name,
                "url": flv_url,
                "type": "flv"
            })

        if not streams:
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}

        return {"streams": streams, "title": name, "avatar": avatar, "isLive": True}
    except Exception:
        logging.exception("[斗鱼] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== B站 ====================
async def parse_bilibili(url, cookie: str = ""):
    """
    【统一 Cookie】cookie 为空时行为与原先完全一致（匿名路径）；
    有 cookie 时携带登录身份，可获取更高画质（如原画/4K）及部分限制内容。
    """
    try:
        rid = url.rstrip("/").split("/")[-1].split("?")[0]
        eff_cookie = cookie or BILI_COOKIE
        hdrs = {"User-Agent": UA, "Referer": "https://live.bilibili.com/"}
        if eff_cookie:
            hdrs["Cookie"] = eff_cookie

        room_resp = await request_with_retry("GET", f"https://api.live.bilibili.com/room/v1/Room/get_info?room_id={rid}", headers=hdrs)
        room_data = room_resp.json()
        if room_data.get("code") != 0:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        real_rid = room_data["data"]["room_id"]

        name, avatar = "B站主播", ""
        try:
            anchor_resp = await request_with_retry("GET",
                f"https://api.live.bilibili.com/live_user/v1/UserInfo/get_anchor_in_room?roomid={rid}",
                headers=hdrs)
            anchor_data = anchor_resp.json()
            if anchor_data.get("code") == 0:
                info = anchor_data.get("data", {}).get("info", {})
                name = info.get("uname") or name
                avatar = info.get("face") or ""
        except Exception:
            pass

        # live_status: 1=直播中, 2=轮播中（轮播也有可播流；与 browse_platforms/bilibili.py 的 in (1,2) 对齐，A6）
        if room_data["data"].get("live_status") not in (1, 2):
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}

        play_resp = await request_with_retry("GET",
            f"https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id={real_rid}&protocol=0,1&format=0,1,2&codec=0,1&qn=10000&platform=web&ptype=8",
            headers=hdrs)
        play = play_resp.json()
        if play.get("code") != 0:
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}
        playurl = play["data"].get("playurl_info", {}).get("playurl", {})
        streams, seen = [], set()
        for stream in playurl.get("stream", []):
            for fmt in stream.get("format", []):
                for codec in fmt.get("codec", []):
                    for info in codec.get("url_info", []):
                        u = info["host"] + codec["base_url"] + info["extra"]
                        if u not in seen:
                            seen.add(u)
                            m = re.search(r"([a-z0-9]+)\.bilivideo", info["host"])
                            streams.append({"cdn": f"{fmt['format_name'].upper()}-{m.group(1) if m else 'cdn'}",
                                           "url": u, "type": "flv" if fmt["format_name"] == "flv" else "m3u8"})
        if not streams:
            return {"streams": [], "isLive": False, "title": name, "avatar": avatar}
        flv_streams = [s for s in streams if s["type"] == "flv"][:3]
        m3u8_streams = [s for s in streams if s["type"] != "flv"][:3]
        streams = flv_streams + m3u8_streams
        return {"streams": streams[:6], "title": name, "avatar": avatar, "isLive": True}
    except Exception:
        logging.exception("[B站] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== 抖音 ====================
async def parse_douyin(url, cookie: str = ""):
    """
    【统一 Cookie】cookie 为空时行为与原先完全一致（匿名路径）；
    有 cookie 时尝试注入到底层 streamget 库（不同版本支持情况可能不同，
    若不支持则自动忽略，不影响主流程）。

    【SSRF 收口】仅接受 douyin.com 域名的 URL：本函数对传入 url 有两处裸抓取
    （DouyinLiveStream.fetch_web_stream_data / room_id 补全时的 request_with_retry 直拉页面），
    旧版靠 _parse_dispatch 的子串判定挡在门外——旁路调用或子串误判
    （https://douyin.com.evil.com/）都会让后端直接 fetch 任意 URL。
    白名单外一律 400，且不发起任何网络请求。
    """
    # 放在 try 之外：非抖音域名是调用方错误（4xx），不该被兜底 except 吞成空结果
    if _site_of_url(url) != "douyin":
        logging.warning(f"[抖音] 拒绝非 douyin.com 域名链接: {_short_url(url)}")
        raise HTTPException(400, "抖音解析仅支持 douyin.com 链接")
    try:
        eff_cookie = cookie or DOUYIN_COOKIE
        # 【兼容性处理】不同版本的 streamget 对 Cookie 的注入方式可能不同，
        # 优先尝试构造函数参数，其次尝试属性注入，两者都失败则忽略。
        try:
            live = DouyinLiveStream(cookie=eff_cookie) if eff_cookie else DouyinLiveStream()
        except TypeError:
            live = DouyinLiveStream()
            if eff_cookie:
                try:
                    live.headers["Cookie"] = eff_cookie
                except Exception:
                    try:
                        live.cookie = eff_cookie
                    except Exception:
                        pass

        data = await live.fetch_web_stream_data(url, process_data=True)

        qualities = ["OD", "UHD", "HD", "SD", "LD"]
        quality_names = {"OD": "原画", "UHD": "蓝光", "HD": "超清", "SD": "高清", "LD": "标清"}

        async def _fetch_quality(q):
            try:
                stream_obj = await live.fetch_stream_url(data, q)
                raw = json.loads(stream_obj.to_json())
                return q, raw.get("flv_url", ""), raw.get("m3u8_url", ""), raw.get("anchor_name", "")
            except Exception as e:
                logging.warning(f"[抖音] 画质 {q} 获取失败: {e}")
                return q, "", "", ""

        results = await asyncio.gather(*[_fetch_quality(q) for q in qualities])

        streams = []
        seen_urls = set()
        seen_qualities = set()
        anchor_name = "抖音主播"
        for q, flv, m3u8, name in results:
            if name and anchor_name == "抖音主播":
                anchor_name = name
            label = quality_names.get(q, q)
            if flv and flv not in seen_urls and label not in seen_qualities:
                seen_urls.add(flv)
                seen_qualities.add(label)
                streams.append({"cdn": f"抖音-{label}", "url": flv, "type": "flv"})
            elif m3u8 and m3u8 not in seen_urls and label not in seen_qualities:
                seen_urls.add(m3u8)
                seen_qualities.add(label)
                streams.append({"cdn": f"抖音-{label}", "url": m3u8, "type": "m3u8"})

        if not streams:
            last_logged = _DOUYIN_NO_STREAM_LOGGED.get(url, 0)
            if time.time() - last_logged > 600:
                logging.warning(f"[抖音] {url} 未获取到任何画质流")
                _DOUYIN_NO_STREAM_LOGGED[url] = time.time()
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}

        quality_order = {"原画": 0, "蓝光": 1, "超清": 2, "高清": 3, "标清": 4}
        streams.sort(key=lambda s: quality_order.get(s["cdn"].replace("抖音-", ""), 99))

        room_id = url.rstrip("/").split("/")[-1].split("?")[0]
        avatar = ""

        def find_avatar_in_dict(d, depth=0):
            if depth > 12:
                return None
            if isinstance(d, dict):
                for k, v in d.items():
                    if k in ("avatarThumb", "avatar_thumb", "avatar_larger", "avatar") and isinstance(v, dict):
                        urls = v.get("urlList") or v.get("url_list") or v.get("url")
                        if isinstance(urls, list) and urls:
                            return urls[0]
                        elif isinstance(urls, str) and urls.startswith("http"):
                            return urls
                    res = find_avatar_in_dict(v, depth + 1)
                    if res:
                        return res
            elif isinstance(d, list):
                for item in d:
                    res = find_avatar_in_dict(item, depth + 1)
                    if res:
                        return res
            return None

        try:
            avatar = find_avatar_in_dict(data) or ""
        except Exception:
            pass

        need_room_id = not room_id.isdigit()
        need_avatar_fallback = not avatar
        if need_room_id or need_avatar_fallback:
            try:
                page_headers = {"User-Agent": UA, "Referer": "https://www.douyin.com/"}
                if eff_cookie:
                    page_headers["Cookie"] = eff_cookie
                resp = await request_with_retry("GET", url, headers=page_headers)
                page_text = resp.text
                if need_room_id:
                    match = re.search(r'"room_id":"(\d+)"', page_text)
                    if match:
                        room_id = match.group(1)
                if need_avatar_fallback:
                    m_render = re.search(r'<script id="RENDER_DATA" type="application/json">([^<]+)</script>', page_text)
                    if m_render:
                        render_json = json.loads(unquote(m_render.group(1)))
                        avatar = find_avatar_in_dict(render_json) or avatar
            except Exception:
                pass

        if avatar and avatar.startswith("//"):
            avatar = "https:" + avatar

        return {"streams": streams, "title": anchor_name,
                "avatar": avatar, "roomId": room_id, "isLive": True}
    except Exception:
        logging.exception("[抖音] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== Twitch ====================
TWITCH_CLIENT_IDS = [
    "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "ue666xxq81dq0l30715w03p3h3a6h",
    "8557m2777l2623943p9951150w621"
]

async def parse_twitch(url, cookie: str = ""):
    try:
        match = re.search(r"twitch\.tv/([^/?]+)", url)
        if not match:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        channel = match.group(1)
        eff_cookie = cookie or TWITCH_COOKIE
        auth_token = ""
        m_at = re.search(r"(?:^|;\s*)auth-token=([^;\s]+)", eff_cookie)
        if m_at:
            auth_token = m_at.group(1)

        proxylist = await get_fixed_proxy_list(EXTERNAL_PROXY_URLS)

        nickname = channel
        avatar = ""
        for client_id in TWITCH_CLIENT_IDS:
            try:
                gql_avatar_payload = [{
                    "operationName": "UserAvatar",
                    "variables": {"login": channel},
                    "query": "query UserAvatar($login: String!) { user(login: $login) { profileImageURL(width: 300) displayName } }"
                }]
                gql_headers = {"Client-ID": client_id, "Content-Type": "application/json", "User-Agent": UA}
                if auth_token:
                    gql_headers["Authorization"] = f"OAuth {auth_token}"
                if eff_cookie:
                    gql_headers["Cookie"] = eff_cookie
                avatar_resp = await request_with_proxy_group("POST", "https://gql.twitch.tv/gql",
                    proxy_list=proxylist, json=gql_avatar_payload, headers=gql_headers, shuffle_proxy=False,
                    log_tag="Twitch-avatar")
                if avatar_resp.status_code == 200:
                    av_data = avatar_resp.json()
                    if isinstance(av_data, list) and av_data[0].get("data", {}).get("user"):
                        user_info = av_data[0]["data"]["user"]
                        avatar = user_info.get("profileImageURL", "")
                        nickname = user_info.get("displayName", channel)
                        break
                logging.warning(f"[Twitch] Client-ID {client_id} 头像查询返回空数据")
            except Exception:
                logging.warning(f"[Twitch] Client-ID {client_id} 头像查询异常")
                continue

        token, sig = None, None
        gql_url = "https://gql.twitch.tv/gql"
        payload = [{"operationName": "PlaybackAccessToken",
                     "variables": {"login": channel, "playerType": "embed"},
                     "query": "query PlaybackAccessToken($login: String!, $playerType: String!) { streamPlaybackAccessToken(channelName: $login, params: { platform: \"web\", playerType: $playerType, playerBackend: \"mediaplayer\" }) { value signature } }"}]
        for client_id in TWITCH_CLIENT_IDS:
            base_headers = {"Client-ID": client_id, "Content-Type": "application/json", "User-Agent": UA}
            if eff_cookie:
                base_headers["Cookie"] = eff_cookie
            header_variants = ([{**base_headers, "Authorization": f"OAuth {auth_token}"}] if auth_token else []) + [base_headers]
            for gql_headers in header_variants:
                try:
                    resp = await request_with_proxy_group("POST", gql_url, proxy_list=proxylist, json=payload,
                                                         headers=gql_headers, shuffle_proxy=False,
                                                         log_tag="Twitch-token")
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            t = data[0].get("data", {}).get("streamPlaybackAccessToken")
                            if t and t.get("value") and t.get("signature"):
                                token, sig = t["value"], t["signature"]
                                break
                    logging.warning(f"[Twitch] Client-ID {client_id} 失效（{'带登录' if 'Authorization' in gql_headers else '匿名'}）")
                except Exception:
                    logging.warning(f"[Twitch] Client-ID {client_id} 请求异常")
            if token and sig:
                break
        if not token or not sig:
            return {"streams": [], "isLive": False, "title": nickname, "avatar": avatar}

        m3u8_url = (f"https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8?sig={sig}&token={quote(token, safe='')}"
                    f"&allow_source=true&allow_audio_only=true&platform=web&player=twitchweb&type=any"
                    f"&p={random.randint(100000, 999999)}&playlist_include_framerate=true&multigroup_video=true"
                    f"&supported_codecs=av1,h265,h264,mp4a&fast_bread=true")
        usher_resp = await request_with_proxy_group("GET", m3u8_url, proxy_list=proxylist,
                                                     headers={"User-Agent": UA, "Referer": "https://player.twitch.tv"},
                                                     shuffle_proxy=False, log_tag="Twitch-m3u8")
        if usher_resp.status_code != 200 or "#EXT-X-STREAM-INF" not in usher_resp.text:
            return {"streams": [], "isLive": False, "title": nickname, "avatar": avatar}

        country_m = re.search(r'USER-COUNTRY="([^"]*)"', usher_resp.text)
        if country_m:
            logging.info(f"[Twitch] usher 出口地区={country_m.group(1)} 登录={'是' if auth_token else '否'}（2K 需两者同时满足）")

        variants, lines = [], usher_resp.text.splitlines()
        for i, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF") or i + 1 >= len(lines):
                continue
            sub_url = lines[i + 1].strip()
            if not sub_url.startswith("http"):
                sub_url = urljoin(m3u8_url, sub_url)
            res_m = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
            vid_m = re.search(r'VIDEO="([^"]*)"', line)
            video = vid_m.group(1) if vid_m else ""
            if res_m:
                name = f"{res_m.group(1)}p{res_m.group(2)}" + ("(源)" if video == "chunked" else "")
                variants.append((int(res_m.group(2)), name, sub_url))
            else:
                variants.append((-1, "仅音频", sub_url))
        variants.sort(key=lambda v: -v[0])
        streams = [{"cdn": f"Twitch-{name}", "url": sub_url, "type": "m3u8"} for _, name, sub_url in variants]
        if not streams:
            return {"streams": [], "isLive": False, "title": nickname, "avatar": avatar}
        return {"streams": streams, "title": nickname, "avatar": avatar, "channelName": channel, "isLive": True}
    except Exception:
        logging.exception("[Twitch] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== SOOP ====================
def _soop_offline_ttl(bj_id: str) -> int:
    count = _SOOP_OFFLINE_COUNT.get(bj_id, 0)
    _SOOP_OFFLINE_COUNT[bj_id] = count + 1
    return min(60 * (2 ** min(count, 4)), 300)

def _soop_reset_offline(bj_id: str):
    _SOOP_OFFLINE_COUNT.pop(bj_id, None)

async def parse_soop(url, cookie: str = ""):
    try:
        eff_cookie = cookie or SOOP_COOKIE
        # 缓存 key 带 cookie 指纹：游客/不同账号的 SOOP 结果不同，避免不同身份互相覆盖串台（C13）
        cookie_tag = f"_auth_{hashlib.md5(eff_cookie.encode('utf-8')).hexdigest()[:8]}" if eff_cookie else ""
        cache_key = url + cookie_tag
        cached = M3U8_CACHE.get(cache_key)
        if cached and cached["expire"] > time.time():
            # P1（缓存深拷贝）：缓存值是跨请求共享的可变结构，下游 normalize_parse 会
            # 原地改 streams 的 url；读出即深拷贝，避免缓存被单条路径污染 + 并发撕裂读。
            return copy.deepcopy(cached["data"])

        parts = url.rstrip('/').split('/')
        bj_id = parts[3].split('?')[0] if len(parts) > 3 else parts[-1].split('?')[0]
        if not bj_id:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}

        headers_pc = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://play.sooplive.com',
            'referer': 'https://play.sooplive.com',
        }
        if eff_cookie:
            headers_pc['cookie'] = eff_cookie

        proxylist = await get_fixed_proxy_list(EXTERNAL_PROXY_URLS)

        nickname = f'BJ-{bj_id}'
        avatar = f'https://stimg.sooplive.com/LOGO/{bj_id[:2]}/{bj_id}/{bj_id}.jpg'

        def _fail(nick, av, offline=True):
            # 【P1二轮：SOOP 退避只认「明确未开播」】offline=True（接口明确返回
            # 未开播/无场次号）才走指数退避 _soop_offline_ttl（60s→300s 封顶）；
            # offline=False（HTTP 非 200/CDN 或 aid 拿不到——网络抖动或上游临时
            # 故障，房间实际可能正在播）固定短缓存 60s 且不递增退避计数，避免
            # 网络抖动被当成「未开播」把发现开播的延迟拖到 5 分钟。
            ttl = _soop_offline_ttl(bj_id) if offline else 60
            result = {"streams": [], "isLive": False, "title": nick, "avatar": av}
            # 【P1二轮：缓存写入侧深拷贝】存入缓存的是副本——返回给首个调用方的
            # 原对象被 normalize_parse 等原地改写时不会渗进缓存（读出侧深拷贝已
            # 在 P1 一轮落地，写侧补齐后缓存对象全程只读）。
            M3U8_CACHE[cache_key] = {"data": copy.deepcopy(result), "expire": time.time() + ttl}
            return result

        live_api = f'https://live.sooplive.com/afreeca/player_live_api.php?bjid={bj_id}'
        live_data_form = {
            'bid': bj_id, 'bno': '', 'type': '', 'pwd': '',
            'player_type': 'html5', 'stream_type': 'common', 'quality': 'master',
            'mode': 'landing', 'from_api': '0', 'is_revive': 'false',
        }
        live_resp = await request_with_proxy_group("POST", live_api, proxy_list=proxylist,
                                                   headers=headers_pc, data=live_data_form, shuffle_proxy=False,
                                                   log_tag="SOOP-live")
        if live_resp.status_code != 200:
            # HTTP 非 200 = 网络/风控层故障，不是「未开播」结论 → 不退避
            return _fail(nickname, avatar, offline=False)
        live_json = live_resp.json()
        channel = live_json.get('CHANNEL', {})
        nickname = channel.get('BJ_NM') or channel.get('BJNICK') or nickname
        result_code = channel.get('RESULT', -1)
        if result_code == -6:
            # 19+ 房间结果短缓存 60s：避免前端反复重试反复打 SOOP 接口（有触发风控风险）（C15）。
            # cookie 已参与 cache_key（见上指纹），之后补填登录 Cookie 会生成新 key，天然绕过本缓存。
            result = {"streams": [], "isLive": False, "error": "19+成年直播间，请在设置中填入SOOP登录Cookie",
                      "title": nickname, "avatar": avatar}
            # 【P1二轮：缓存写入侧深拷贝】同 _fail——存副本防调用方原地改写渗入缓存
            M3U8_CACHE[cache_key] = {"data": copy.deepcopy(result), "expire": time.time() + 60}
            return result
        if result_code not in [0, 1]:
            return _fail(nickname, avatar)

        broad_no = channel.get('BNO', '')
        if not broad_no:
            # RESULT 正常但无场次号 = 接口明确表示当前没有直播场次 → 计入退避
            return _fail(nickname, avatar)

        ts_now = time.time()
        cdn_params = {
            'return_type': 'gcp_cdn',
            'use_cors': 'false',
            'cors_origin_url': 'play.sooplive.com',
            'broad_key': f'{broad_no}-common-master-hls',
            'time': str(ts_now),
        }
        cdn_resp = await request_with_proxy_group("GET",
            'http://livestream-manager.sooplive.com/broad_stream_assign.html',
            proxy_list=proxylist, headers=headers_pc, params=cdn_params, shuffle_proxy=False,
            log_tag="SOOP-cdn")
        if cdn_resp.status_code != 200:
            # 已确认开播，只是 CDN 分配失败 → 网络层故障不退避
            return _fail(nickname, avatar, offline=False)
        cdn_json = cdn_resp.json()
        view_url = cdn_json.get('view_url')
        if not view_url:
            # 已确认开播，只是 CDN 分配失败 → 不退避
            return _fail(nickname, avatar, offline=False)

        aid_form = live_data_form.copy()
        aid_form['type'] = 'aid'
        aid_resp = await request_with_proxy_group("POST", live_api, proxy_list=proxylist,
                                                  headers=headers_pc, data=aid_form, shuffle_proxy=False,
                                                  log_tag="SOOP-aid")
        if aid_resp.status_code != 200:
            # 已确认开播，aid 接口网络层失败 → 不退避
            return _fail(nickname, avatar, offline=False)
        aid_json = aid_resp.json()
        aid = aid_json.get('CHANNEL', {}).get('AID', '')
        if not aid:
            # 已确认开播，aid 拿不到 → 不退避
            return _fail(nickname, avatar, offline=False)

        m3u8_url = f'{view_url}?aid={aid}'
        streams = []
        try:
            master_resp = await request_with_proxy_group("GET", m3u8_url, proxy_list=proxylist,
                                                         headers={"User-Agent": UA, "Referer": "https://play.sooplive.com"},
                                                         shuffle_proxy=False, log_tag="SOOP-master")
            if master_resp.status_code == 200:
                streams = parse_multivariant_m3u8(master_resp.text, m3u8_url, "SOOP")
        except Exception:
            pass
        if not streams:
            streams = [{"cdn": "SOOP-Source", "url": m3u8_url, "type": "m3u8"}]

        result = {"streams": streams, "title": nickname, "avatar": avatar, "isLive": True,
                   "danmaku": {"platform": "soop", "roomId": bj_id}}
        _soop_reset_offline(bj_id)
        # 【P1二轮：缓存写入侧深拷贝】存副本，调用方原地改写不渗入缓存
        M3U8_CACHE[cache_key] = {"data": copy.deepcopy(result), "expire": time.time() + 15}
        return result
    except Exception:
        logging.exception("[SOOP] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

# ==================== PandaTV ====================
async def parse_panda_manual(url):
    try:
        cached = M3U8_CACHE.get(url)
        if cached and cached["expire"] > time.time():
            # P1（缓存深拷贝）：同 parse_soop——读出即拷贝，缓存对象只读不写。
            return copy.deepcopy(cached["data"])

        user_id = url.split('?')[0].rstrip('/').split('/')[-1]
        headers = {'origin': 'https://www.pandalive.co.kr', 'referer': 'https://www.pandalive.co.kr/', 'user-agent': UA}
        proxylist = await get_fixed_proxy_list(EXTERNAL_PROXY_URLS)

        info_url = 'https://api.pandalive.co.kr/v1/member/bj'
        resp = await request_with_proxy_group("POST", info_url, proxy_list=proxylist,
                                              headers=headers,
                                              data={'userId': user_id, 'info': 'media fanGrade'},
                                              shuffle_proxy=False, log_tag="PandaTV-info")
        if resp.status_code != 200:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        info_json = resp.json()
        if 'bjInfo' not in info_json:
            return {"streams": [], "isLive": False, "title": "", "avatar": ""}
        bj_info = info_json.get('bjInfo', {})
        anchor_name = bj_info.get('nick', user_id)

        _IMG_FIELDS = (
            'thumbUrl', 'profileImg', 'profileImage', 'img', 'userImg', 'thumbImg',
            'bjImg', 'thumb', 'photo', 'avatar', 'iconImg', 'userPic',
            'thumbnail', 'profile', 'profileThumb',
        )
        avatar = next((bj_info[k] for k in _IMG_FIELDS if bj_info.get(k)), '')

        if 'media' not in info_json:
            result = {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}
            # 【P1二轮：缓存写入侧深拷贝】存副本，调用方原地改写不渗入缓存
            M3U8_CACHE[url] = {"data": copy.deepcopy(result), "expire": time.time() + 60}
            return result

        play_url = 'https://api.pandalive.co.kr/v1/live/play'
        resp2 = await request_with_proxy_group("POST", play_url, proxy_list=proxylist,
                                               headers=headers,
                                               data={'action': 'watch', 'userId': user_id, 'password': '', 'shareLinkType': ''},
                                               shuffle_proxy=False, log_tag="PandaTV-play")
        if resp2.status_code != 200:
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}
        play_json = resp2.json()
        if 'errorData' in play_json:
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}
        hls_list = (play_json.get('PlayList') or {}).get('hls')
        # 只检查键存在不够：hls 可能是空数组/非列表/元素无 url，原写法会 IndexError 被外层吞掉（E26）
        if not isinstance(hls_list, list) or not hls_list or not isinstance(hls_list[0], dict) \
                or not hls_list[0].get('url'):
            return {"streams": [], "isLive": False, "title": anchor_name, "avatar": avatar}
        real_m3u8 = hls_list[0]['url']

        if not avatar or 'default' in avatar.lower() or 'no_image' in avatar.lower():
            for _section in ('bjInfo', 'userInfo', 'bjProfile', 'channelInfo', 'mediaInfo'):
                _d = play_json.get(_section)
                if isinstance(_d, dict):
                    _candidate = next((str(_d[k]) for k in _IMG_FIELDS if _d.get(k)), '')
                    if _candidate:
                        avatar = _candidate
                        logging.info(f"[PandaTV] 从 play_json[{_section}] 获取到头像")
                        break

        def _fix_avatar_url(u):
            if not u: return ''
            if u.startswith('//'): return 'https:' + u
            if not u.startswith('http'):
                return 'https://profile.pandalive.co.kr' + ('/' if not u.startswith('/') else '') + u
            return u
        avatar = _fix_avatar_url(avatar)

        streams = []
        try:
            cf_worker = CF_WORKER.rstrip("/") if CF_WORKER else ""
            if cf_worker:
                fetch_url = f"{cf_worker}?url={quote(real_m3u8, safe='')}&referer={quote('https://www.pandalive.co.kr/', safe='')}"
                master_resp = await request_with_proxy_group("GET", fetch_url, proxy_list=[None],
                                                             headers={"User-Agent": UA}, shuffle_proxy=False,
                                                             log_tag="PandaTV-master")
            else:
                master_resp = await request_with_proxy_group("GET", real_m3u8, proxy_list=proxylist,
                                                             headers={"User-Agent": UA, "Referer": "https://www.pandalive.co.kr/",
                                                                      "Origin": "https://www.pandalive.co.kr"},
                                                             shuffle_proxy=False, log_tag="PandaTV-master")
            if master_resp.status_code == 200:
                streams = parse_multivariant_m3u8(master_resp.text, real_m3u8, "PandaTV")
        except Exception:
            pass
        if not streams:
            streams = [{"cdn": "PandaTV-Source", "url": real_m3u8, "type": "m3u8"}]

        result = {"streams": streams, "title": anchor_name, "avatar": avatar, "isLive": True}
        # 【P1二轮：缓存写入侧深拷贝】存副本，调用方原地改写不渗入缓存
        M3U8_CACHE[url] = {"data": copy.deepcopy(result), "expire": time.time() + 15}
        return result
    except Exception:
        logging.exception("[PandaTV] 解析异常")
        return {"streams": [], "isLive": False, "title": "", "avatar": ""}

@app.post("/api/follows/batch")
@limiter.limit("20/minute")
async def api_follows_batch(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "请求体必须是 JSON")

    items = body.get("items", [])
    if not isinstance(items, list) or not items:
        raise HTTPException(400, "items 不能为空")
    if len(items) > 30:
        raise HTTPException(400, "单次最多查询 30 个")

    # P0-1：海外/国内分池改统一 hostname 判定（原 any(m in url) 子串，
    # URL 里任意位置出现 twitch.tv 都会被当成海外房间）
    sem_domestic = asyncio.Semaphore(5)
    sem_foreign = asyncio.Semaphore(5)

    async def _one(item):
        url = item.get("url", "")
        cookie = item.get("cookie", "")
        sem = sem_foreign if _host_in(url, FOREIGN_ROOM_DOMAINS) else sem_domestic
        async with sem:
            try:
                result = await _parse_dispatch(url, cookie)
            except HTTPException as e:
                result = {"streams": [], "isLive": False, "title": "", "avatar": "", "error": str(e.detail)}
            except Exception as e:
                result = {"streams": [], "isLive": False, "title": "", "avatar": "", "error": str(e)}
        # 【分流补齐】批量结果的头像也要过 Worker /avatar 包装（原 normalize 只包了流）：
        # 前端 useFollowsBatchRefresh 会把 res.avatar 写进关注列表/开播 toast，海外裸链国内必裂
        try:
            from browse_api import _wrap_image_url
            if result.get("avatar"):
                result["avatar"] = _wrap_image_url(str(result["avatar"]))
        except Exception:
            pass
        result["url"] = url
        return result

    # 整体超时兜底：各单房间底层请求有 15s 超时，但斗鱼多线路×画质组合仍可能拖长整批（C14）；
    # 超时返回 504，前端 useFollowsBatchRefresh 捕获后会自动退化为 /api/parse 逐个查询（原版同款兜底）
    batch_timeout = float(os.getenv("FOLLOWS_BATCH_TIMEOUT", "45"))
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(_one(it) for it in items)), timeout=batch_timeout
        )
    except asyncio.TimeoutError:
        logging.warning(f"[关注批量] {len(items)} 个房间整体超时(>{batch_timeout}s)")
        raise HTTPException(504, f"批量解析整体超时(>{batch_timeout}s)，请稍后重试")
    return {"results": results}

# 【健壮性】/api/parse 总期限：各底层请求有 15s 超时，但代理轮换×多线路组合仍可能
# 累计成超长挂起（前端 useFetch 20s 会先掩；这里防协程长期堆积，60s 兜底可 env 调）
PARSE_TOTAL_TIMEOUT = float(os.getenv("PARSE_TOTAL_TIMEOUT", "60"))

@app.get("/api/parse")
@limiter.limit("60/minute")
async def api_parse(request: Request, url: str = Query(...)):
    # P0-7/8：Cookie 改 x-platform-cookie 头（旧 ?cookie= 查询串会落反代/服务日志
    # 与浏览器历史；与 browse 路由的 _cookie_for 同款头，前后端同版本发布）
    cookie = request.headers.get("x-platform-cookie", "")
    try:
        data = await asyncio.wait_for(_parse_dispatch(url, cookie), timeout=PARSE_TOTAL_TIMEOUT)
        from browse_api import normalize_parse
        from browse_engine import resolve_site
        site, room_id = resolve_site(url)
        return normalize_parse(data, site or "", room_id or "")
    except asyncio.TimeoutError:
        raise HTTPException(504, f"解析超时(>{PARSE_TOTAL_TIMEOUT:.0f}s)，请稍后重试")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


def _browse_identity(url: str) -> tuple[str, str]:
    # P0-1：主机名识别改用统一 PLATFORM_SITES（原函数自带一份同内容 mappings 副本）
    site = _site_of_url(url)
    segments = [s for s in (urlparse(url).path or "").strip("/").split("/") if s]
    if not segments:
        return site, ""
    # SOOP 的 URL 是 /{bj_id} 或 /{bj_id}/{broad_no}：房间标识是第一段 bj_id（字母数字混合，不能取末段）
    if site == "soop":
        return site, segments[0]
    return site, segments[-1]

async def _parse_dispatch(url: str, cookie: str = ""):
    """
    【统一 Cookie】所有平台的解析函数都接受 cookie 参数；
    具体哪个平台用得上、用不用，由各 parse_xxx 内部决定，入口这里统一透传。

    【P0-1 SSRF 收口】分派改 hostname 白名单（PLATFORM_SITES）：旧版 `if "douyin.com" in url`
    子串匹配，https://douyin.com.evil.com/、https://evil.com/?q=douyin.com 都能混进对应
    parse_xxx，随后被后端直接 fetch（任意 SSRF 目标）。现在主机名未命中平台白名单
    一律 400，且要求 URL 带 scheme（前端两条入口都会补 https://）。
    """
    try:
        site = _site_of_url(url)
        if site == "huya": return await parse_huya(url, cookie=cookie)
        if site == "douyu": return await parse_douyu(url, cookie=cookie)
        if site == "bilibili": return await parse_bilibili(url, cookie=cookie)
        if site == "douyin": return await parse_douyin(url, cookie=cookie)
        if site == "twitch": return await parse_twitch(url, cookie=cookie)
        if site == "soop": return await parse_soop(url, cookie=cookie)
        if site == "pandatv": return await parse_panda_manual(url)
        raise HTTPException(400, "不支持的平台")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# Browse routes share this FastAPI app, parser, proxy configuration and process.
from browse_api import router as browse_router, configure_parser as _configure_browse_parser
_configure_browse_parser(_parse_dispatch)
app.include_router(browse_router)

# ==================== 弹幕代理 ====================

def _reset_go_service_check(mark_unavailable: bool = False):
    global _GO_SERVICE_AVAILABLE, _GO_SERVICE_LAST_CHECK
    _GO_SERVICE_LAST_CHECK = 0
    if mark_unavailable:
        _GO_SERVICE_AVAILABLE = False

def _ws_is_open(ws) -> bool:
    """websockets 新旧版本的连接状态判断（legacy: ws.state.name；asyncio 实现(>=13): ws.protocol.state.name）。
    拿不到状态属性时按已连接处理，发送失败由上层 except 兜底，不影响主流程（C12）。"""
    state = getattr(ws, "state", None)
    if state is None:
        state = getattr(getattr(ws, "protocol", None), "state", None)
    return getattr(state, "name", "") == "OPEN"

def _ws_origin_allowed(websocket: WebSocket) -> bool:
    if not ALLOWED_WS_ORIGINS:
        return True
    origin = websocket.headers.get("origin", "")
    if not origin:
        return True
    return origin in ALLOWED_WS_ORIGINS


# ==================== WS 一次性 Ticket + 路径参数校验（P0-7/8） ====================
# 【Cookie 不走查询串】浏览器 WebSocket API 不支持自定义请求头，SOOP 19+ 房间的登录
# Cookie 此前只能拼在 ?cookie= 里——反代访问日志/服务日志/浏览器历史全都会留下完整
# 凭据。改为两段式：前端先带 x-platform-cookie 头调 /api/ws-ticket 换一张一次性
# ticket（60s 有效、用后即焚、绑定 site+room_id），再用 ?ticket= 连 WS；查询串里
# 只出现随机 ticket，凭据本身零落盘。CORS 无需改动：app 已 allow_headers=["*"]，
# 自定义头跨域可用。
#
# 【路径参数校验】三个弹幕 WS 端点的 room_id/channel_name 此前零校验直接进下游：
#   douyin → GO_DANMAKU_BASE/ws/{room_id}（路径拼接，..%2F 可注入）；
#   twitch → JOIN #{channel_name}（IRC 命令注入：\r\n 可拆出任意 IRC 命令）；
#   soop → player_live_api.php?bjid={room_id}（查询参数注入）+ /Websocket/{room_id}（路径拼接）。
# 现在按平台字符集白名单校验，不命中一律 close(1008)（与 Origin 拒绝同款）。
_WS_ROOM_PATTERNS: dict = {
    "douyin": re.compile(r"^\d{1,20}$"),            # 房间号纯数字
    "twitch": re.compile(r"^[A-Za-z0-9_]{3,25}$"),   # 频道登录名字符集（防 \r\n IRC 注入）
    "soop": re.compile(r"^[A-Za-z0-9_]{2,30}$"),     # bj_id 字母数字（防 bjid 参数/路径注入）
}


def _valid_ws_room(site: str, value: str) -> bool:
    """弹幕 WS 路径参数校验：按平台字符集白名单，未登记站点一律拒绝。"""
    pat = _WS_ROOM_PATTERNS.get(site)
    return bool(pat) and bool(pat.match(value or ""))


_WS_TICKET_TTL_SECONDS = 60
_WS_TICKETS: dict = {}


def _sweep_ws_tickets(now: float | None = None) -> None:
    """清理过期 ticket（签发时顺带扫，存量有 TTL 兜底不会无限增长）。"""
    now = time.time() if now is None else now
    for k in [k for k, v in _WS_TICKETS.items() if v["expire"] < now]:
        _WS_TICKETS.pop(k, None)


def _issue_ws_ticket(site: str, room_id: str, cookie: str) -> str:
    """签发一次性 ticket（60s 有效、绑定 site+room_id，凭据存服务端不下发）。"""
    now = time.time()
    _sweep_ws_tickets(now)
    ticket = secrets.token_urlsafe(32)
    _WS_TICKETS[ticket] = {
        "site": site, "room": room_id, "cookie": cookie,
        "expire": now + _WS_TICKET_TTL_SECONDS,
    }
    return ticket


def _consume_ws_ticket(ticket: str, site: str, room_id: str) -> str | None:
    """单次消费：先 pop（无论校验是否通过都用掉，防重放试探）。
    返回 None = 无效/过期/已用/站点房间不匹配，调用方应拒绝连接（1008）；
    与「没带 ticket 的匿名连接」区分开（后者 cookie 为空串，照常匿名连）。"""
    if not ticket:
        return None
    entry = _WS_TICKETS.pop(ticket, None)
    if not entry:
        return None
    if entry["expire"] < time.time():
        return None
    if entry["site"] != site or entry["room"] != room_id:
        return None
    return entry["cookie"]


@app.get("/api/ws-ticket")
@limiter.limit("30/minute")
async def api_ws_ticket(request: Request, site: str = Query(...), room_id: str = Query(...)):
    """弹幕 WS 一次性 ticket：Cookie 经 x-platform-cookie 头进来，只返回随机 ticket。
    浏览器 WS 不能带自定义头、Cookie 拼 ?cookie= 会落反代/服务日志与浏览器历史
    （P0-7/8），故先换票再连接；ticket 60s 单次有效。"""
    if site not in _WS_ROOM_PATTERNS:
        raise HTTPException(400, "不支持的弹幕站点")
    if not _valid_ws_room(site, room_id):
        raise HTTPException(400, "room_id 格式非法")
    cookie = request.headers.get("x-platform-cookie", "")
    return {"ticket": _issue_ws_ticket(site, room_id, cookie), "expires_in": _WS_TICKET_TTL_SECONDS}

@app.websocket("/ws/douyin/{room_id}")
async def websocket_douyin_danmaku(websocket: WebSocket, room_id: str):
    # P0-7/8：路径参数白名单校验（此前 ..%2F 等可注入 Go 服务 URL 路径拼接）
    if not _valid_ws_room("douyin", room_id):
        logging.warning(f"[WS] 抖音弹幕拒绝非法 room_id: {room_id!r}")
        await websocket.close(code=1008)
        return
    if not _ws_origin_allowed(websocket):
        logging.warning(f"[WS] 抖音弹幕拒绝非法 Origin: {websocket.headers.get('origin')!r}")
        await websocket.close(code=1008)
        return

    await websocket.accept()

    if not await _check_go_service():
        try:
            await websocket.send_text(json.dumps({
                "method": "error",
                "content": "弹幕服务暂时不可用，请稍后重试"
            }))
            await websocket.close(code=1013)
        except Exception:
            pass
        return

    go_ws_url = f"{GO_DANMAKU_BASE}/ws/{room_id}"
    try:
        async with websockets.connect(go_ws_url, ping_interval=None) as go_ws:
            async def forward_to_go():
                try:
                    while True:
                        data = await websocket.receive_text()
                        if data == 'ping':
                            try:
                                await go_ws.ping()
                            except Exception:
                                pass
                            await websocket.send_text('pong')
                        elif _ws_is_open(go_ws):
                            await go_ws.send(data)
                except WebSocketDisconnect:
                    pass
                except Exception as e:
                    logging.debug(f"[WS] forward_to_go 异常: {e}")

            async def forward_to_frontend():
                try:
                    while True:
                        try:
                            data = await asyncio.wait_for(go_ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            # 【健壮性】冷清房 60s 无弹幕是正常态，不等于死链（原先直接
                            # 断开重连，冷清房每分钟断一次刷系统消息）。改为协议层探活：
                            # ping 后限时等 pong（pong 由 websockets 库在协议层消化，
                            # recv() 拿不到，须等 ping() 返回的 waiter）；pong 回来即链路
                            # 健康继续收，探活失败才判死断开。
                            async def _await_pong():
                                await pong
                            try:
                                pong = await go_ws.ping()
                            except Exception:
                                pong = None
                            if pong is not None:
                                try:
                                    await asyncio.wait_for(_await_pong(), timeout=10)
                                    continue
                                except Exception:
                                    pass
                            _reset_go_service_check()
                            logging.warning(f"[WS] Go 服务链路探活失败 (room={room_id})，关闭连接")
                            break
                        if isinstance(data, bytes):
                            data = data.decode("utf-8")
                        await websocket.send_text(data)
                except Exception as e:
                    logging.debug(f"[WS] forward_to_frontend 异常: {e}")

            task_go = asyncio.create_task(forward_to_go())
            task_fe = asyncio.create_task(forward_to_frontend())
            done, pending = await asyncio.wait(
                [task_go, task_fe],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
    except websockets.exceptions.ConnectionClosed:
        logging.info(f"[WS] Go 服务连接已关闭 (room={room_id})")
        _reset_go_service_check()
    except OSError as e:
        _reset_go_service_check(mark_unavailable=True)
        logging.warning(f"[WS] 抖音代理无法连接 Go 服务: {e}")
    except Exception as e:
        logging.warning(f"[WS] 抖音代理异常: {e}")
    finally:
        # starlette 的 client_state 枚举是 CONNECTED/CONNECTING/DISCONNECTED（无 OPEN），
        # 原判断永远为 False → 连接从不主动关闭；改为直接 close，与其他 WS 端点 finally 一致（C12）
        try:
            await websocket.close()
        except Exception:
            pass

_SOOP_ESC = b'\x1b\x09'
_SOOP_F = b'\x0c'


def _soop_build_frame(service: int, body: bytes) -> bytes:
    header = _SOOP_ESC + f"{service:04d}".encode("ascii") + f"{len(body):06d}".encode("ascii") + b"00"
    return header + body


_SOOP_ELEM_START = b'\x11'
_SOOP_ELEM_END = b'\x12'
_SOOP_SPACE = b'\x06'


def _soop_cookie_field(cookie: str, name: str) -> str:
    if not cookie:
        return ""
    for part in cookie.split(";"):
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        if k.strip() == name:
            return v.strip()
    return ""


def _soop_connect_packet(auth_ticket: str = "") -> bytes:
    if auth_ticket:
        body = _SOOP_F + auth_ticket.encode("utf-8") + _SOOP_F * 2 + b"16" + _SOOP_F
    else:
        body = _SOOP_F * 3 + b"16" + _SOOP_F
    return _soop_build_frame(1, body)


def _soop_log_query(meta: dict) -> bytes:
    out = b""
    for k, v in meta.items():
        out += _SOOP_SPACE + b"&" + _SOOP_SPACE + str(k).encode("utf-8") + _SOOP_SPACE + b"=" + _SOOP_SPACE + str(v).encode("utf-8")
    return out


def _soop_join_packet(chat_no: str, ftk: str = "", _au: str = "", log_meta: dict = None) -> bytes:
    chat_no_bytes = chat_no.encode("utf-8")
    if ftk:
        meta = log_meta or {}
        query = _soop_log_query({
            "set_bps": meta.get("set_bps", "0"),
            "view_bps": meta.get("view_bps", "0"),
            "quality": "normal",
            "uuid": _au or "",
            "geo_cc": meta.get("geo_cc", ""),
            "geo_rc": meta.get("geo_rc", ""),
            "acpt_lang": meta.get("acpt_lang", ""),
            "svc_lang": meta.get("svc_lang", ""),
            "subscribe": 0,
            "lowlatency": 0,
            "mode": "landing",
        })
        body = (_SOOP_F + chat_no_bytes + _SOOP_F + ftk.encode("utf-8") + _SOOP_F + b"0" + _SOOP_F
                + b"log" + _SOOP_ELEM_START + query + _SOOP_ELEM_END
                + b"pwd" + _SOOP_ELEM_START + _SOOP_ELEM_END
                + b"auth_info" + _SOOP_ELEM_START + b"NULL" + _SOOP_ELEM_END
                + b"pver" + _SOOP_ELEM_START + b"2" + _SOOP_ELEM_END
                + b"access_system" + _SOOP_ELEM_START + b"html5" + _SOOP_ELEM_END
                + _SOOP_F)
    else:
        body = _SOOP_F + chat_no_bytes + _SOOP_F * 5
    return _soop_build_frame(2, body)


def _soop_enter_info_packet(syn_ack: str) -> bytes:
    body = _SOOP_F + syn_ack.encode("utf-8") + _SOOP_F + b"0" + _SOOP_F
    return _soop_build_frame(12, body)


def _soop_heartbeat_packet() -> bytes:
    return _soop_build_frame(0, _SOOP_F)


def _soop_parse_frames(data: bytes):
    frames = []
    offset = 0
    header_len = 14
    while offset + header_len <= len(data):
        if data[offset:offset + 2] != _SOOP_ESC:
            break
        try:
            service = int(data[offset + 2:offset + 6].decode("ascii", errors="ignore"))
            body_len = int(data[offset + 6:offset + 12].decode("ascii", errors="ignore"))
        except ValueError:
            break
        if body_len < 0:
            break
        packet_end = offset + header_len + body_len
        if packet_end > len(data):
            break
        frames.append((service, data[offset + header_len:packet_end]))
        offset = packet_end
    return frames


def _soop_decode_chat(body: bytes):
    parts = body.split(_SOOP_F)
    fields = [p.decode("utf-8", errors="ignore") for p in parts]
    if len(fields) <= 6:
        return None
    comment = fields[1].strip()
    nick = fields[6].strip()
    if not comment or not nick or comment in ("-1", "1") or "|" in comment:
        return None
    return nick, comment


# ==================== Twitch 弹幕 WS 桥接 ====================
# 【本批修复】Twitch IRC-over-WebSocket 一个帧可含多条以 \r\n 分隔的 IRC 消息
# （官方 chatbot 文档明说消息以 \r\n 分隔，一帧多条是常态）；此前整帧只
# re.match 首条，同帧其余弹幕全部丢失，且 PING 只查帧首、夹在中间不回
# PONG 会被 Twitch 断连。拆成纯函数便于单测（backend/tests/test_twitch_ws.py）。
_TWITCH_PRIVMSG_RE = re.compile(r":(\w+)!\w+@\w+\.tmi\.twitch\.tv PRIVMSG #\w+ :(.*)")


def _split_twitch_irc_frame(frame: str) -> list[str]:
    """拆帧：\r\n 分隔为主，兼容只用 \n 的宽松实现；过滤纯空白行。"""
    return [line for line in frame.replace("\r\n", "\n").split("\n") if line.strip()]


def _is_twitch_ping(line: str) -> bool:
    """IRC 层 PING（如 `PING :tmi.twitch.tv`），须回 PONG 否则被断连。"""
    return line.startswith("PING")


def _parse_twitch_privmsg(line: str) -> tuple[str, str] | None:
    m = _TWITCH_PRIVMSG_RE.match(line)
    return (m.group(1), m.group(2)) if m else None


@app.websocket("/ws/twitch/{channel_name}")
async def websocket_twitch_danmaku(ws_conn: WebSocket, channel_name: str):
    # P0-7/8：频道名白名单校验（此前 \r\n 等可拆出任意 IRC 命令注入 JOIN）
    if not _valid_ws_room("twitch", channel_name):
        logging.warning(f"[Twitch WS] 拒绝非法 channel_name: {channel_name!r}")
        await ws_conn.close(code=1008)
        return
    if not _ws_origin_allowed(ws_conn):
        logging.warning(f"[Twitch WS] 拒绝非法 Origin: {ws_conn.headers.get('origin')!r}")
        await ws_conn.close(code=1008)
        return

    await ws_conn.accept()
    twitch_ws_url = "wss://irc-ws.chat.twitch.tv:443"
    # 【健壮性】握手失败不再静默：原先上游 connect 异常只落日志 + finally 无码 close，
    # 前端只看到 1006，不知道原因只能盲重连。失败先 send_json error 告知前端（弹幕区
    # 显示原因），再 close(1011)（前端指数退避重连，不再 3s 死循环）。
    handshake_failed = False
    try:
        async with websockets.connect(twitch_ws_url, ping_interval=30, ping_timeout=10) as twitch_ws:
            await twitch_ws.send("PASS SCHMOOPIIE")
            await twitch_ws.send(f"NICK justinfan{random.randint(10000, 99999)}")
            await twitch_ws.send(f"JOIN #{channel_name.lower()}")

            async def forward_to_frontend():
                try:
                    async for frame in twitch_ws:
                        # 一帧多条 IRC 消息逐行处理：此前只匹配首条，其余弹幕全部丢失
                        for line in _split_twitch_irc_frame(frame):
                            # PING 夹在帧中间同样回 PONG（此前只查帧首）
                            if _is_twitch_ping(line):
                                await twitch_ws.send("PONG :tmi.twitch.tv")
                                continue
                            parsed = _parse_twitch_privmsg(line)
                            if not parsed:
                                continue
                            nick, content = parsed
                            try:
                                await ws_conn.send_json({"type": "chat", "nick": nick, "content": content})
                            except Exception:
                                # 前端已断开：直接退出转发（此前内层 break 后
                                # 外层 async for 仍继续空转直到上游断开）
                                return
                except Exception as e:
                    logging.debug(f"[Twitch WS] forward_to_frontend 异常: {e}")

            async def listen_to_frontend():
                try:
                    while True:
                        data = await ws_conn.receive_text()
                        if data == "ping":
                            try:
                                await ws_conn.send_text("pong")
                            except Exception:
                                break
                except WebSocketDisconnect:
                    pass
                except Exception as e:
                    logging.debug(f"[Twitch WS] listen_to_frontend 异常: {e}")

            task_fwd = asyncio.create_task(forward_to_frontend())
            task_lsn = asyncio.create_task(listen_to_frontend())
            done, pending = await asyncio.wait(
                [task_fwd, task_lsn],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    except Exception as e:
        handshake_failed = True
        logging.warning(f"[Twitch WS] 连接异常: {e}")
        try:
            await ws_conn.send_json({"type": "error", "message": f"弹幕服务器连接失败：{type(e).__name__}"})
        except Exception:
            pass
    finally:
        try:
            await ws_conn.close(code=1011 if handshake_failed else 1000)
        except Exception:
            pass

@app.websocket("/ws/soop/{room_id}")
async def websocket_soop_danmaku(ws_conn: WebSocket, room_id: str, ticket: str = Query("")):
    # P0-7/8：路径参数白名单校验（此前 bjid 查询参数注入 + /Websocket/ 路径注入）
    if not _valid_ws_room("soop", room_id):
        logging.warning(f"[SOOP WS] 拒绝非法 room_id: {room_id!r}")
        await ws_conn.close(code=1008)
        return
    if not _ws_origin_allowed(ws_conn):
        logging.warning(f"[SOOP WS] 拒绝非法 Origin: {ws_conn.headers.get('origin')!r}")
        await ws_conn.close(code=1008)
        return

    # P0-7/8：Cookie 改经一次性 ticket 传入（旧 ?cookie= 会落反代/服务日志与浏览器历史）。
    # 带 ticket 但无效/过期/已用/房间不匹配 → 拒绝（1008）；不带 ticket → 匿名连接
    # （19+ 房间会收到后端错误提示），与旧匿名行为兼容；ticket 对应空 Cookie 也照常匿名。
    cookie = ""
    if ticket:
        consumed = _consume_ws_ticket(ticket, "soop", room_id)
        if consumed is None:
            logging.warning(f"[SOOP WS] room={room_id} ticket 无效/过期/已用/房间不匹配，拒绝连接")
            await ws_conn.close(code=1008)
            return
        cookie = consumed
    await ws_conn.accept()
    # 【健壮性】握手/上游失败标记：finally 据此选 close code（1011=失败，1000=正常）
    handshake_failed = False
    try:
        headers_pc = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://play.sooplive.com',
            'referer': 'https://play.sooplive.com',
        }
        if cookie:
            headers_pc['cookie'] = cookie
        live_api = f'https://live.sooplive.com/afreeca/player_live_api.php?bjid={room_id}'
        live_data_form = {
            'bid': room_id, 'bno': '', 'type': '', 'pwd': '',
            'player_type': 'html5', 'stream_type': 'common', 'quality': 'master',
            'mode': 'landing', 'from_api': '0', 'is_revive': 'false',
        }
        proxylist = await get_fixed_proxy_list(EXTERNAL_PROXY_URLS)
        live_resp = await request_with_proxy_group("POST", live_api, proxy_list=proxylist,
                                                     headers=headers_pc, data=live_data_form, shuffle_proxy=False,
                                                     log_tag="SOOP-danmaku-meta")
        channel = live_resp.json().get('CHANNEL', {})
        chat_no = str(channel.get('CHATNO', '')).strip()
        auth_ticket = _soop_cookie_field(cookie, 'AuthTicket')
        au = _soop_cookie_field(cookie, '_au')
        ftk = str(channel.get('FTK', '') or '').strip()
        view_bps = '0'
        try:
            presets = channel.get('VIEWPRESET')
            if isinstance(presets, list) and presets and isinstance(presets[0], dict):
                view_bps = str(presets[0].get('bps', '0') or '0')
        except Exception:
            pass
        log_meta = {
            'set_bps': str(channel.get('BPS', '0') or '0'),
            'view_bps': view_bps,
            'geo_cc': str(channel.get('geo_cc', '') or ''),
            'geo_rc': str(channel.get('geo_rc', '') or ''),
            'acpt_lang': str(channel.get('acpt_lang', '') or ''),
            'svc_lang': str(channel.get('svc_lang', '') or ''),
        }
        if not chat_no:
            msg = "SOOP 聊天室号获取失败"
            if not cookie:
                msg += "：若为 19+ 直播间，请在设置页填写 SOOP 登录 Cookie 后重试"
            await ws_conn.send_json({"type": "error", "message": msg})
            # 【健壮性】明确拒绝用 1008：前端收到会停止自动重连（此前无码 close=1000，
            # 前端 3s 换票死循环重试一个注定失败的连接）
            await ws_conn.close(code=1008)
            return

        host = str(channel.get('CHDOMAIN', '') or '').strip()
        if not host:
            raw_ip = str(channel.get('CHIP', '') or '').strip()
            octets = raw_ip.split('.')
            if len(octets) == 4 and all(o.isdigit() and 0 <= int(o) <= 255 for o in octets):
                encoded = ''.join(f'{int(o):02X}' for o in octets)
                host = f'chat-{encoded}.sooplive.com'
        try:
            plain_port = int(channel.get('CHPT', 0))
        except (ValueError, TypeError):
            plain_port = 0
        if not host or plain_port <= 0 or plain_port >= 65535:
            await ws_conn.send_json({"type": "error", "message": "SOOP 弹幕连接地址解析失败"})
            await ws_conn.close(code=1008)
            return

        soop_ws_url = f"wss://{host}:{plain_port + 1}/Websocket/{room_id}"
        logging.info(f"[SOOP WS] room={room_id} chatNo={chat_no} host={host} plainPort={plain_port} "
                     f"auth={'on' if auth_ticket else 'off'} ftk={'y' if ftk else 'n'} → {soop_ws_url}")
        ws_headers = {
            "Origin": "https://play.sooplive.co.kr",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        }
        if cookie:
            ws_headers["Cookie"] = cookie

        try:
            soop_ws_ctx = websockets.connect(
                soop_ws_url, subprotocols=["chat"], additional_headers=ws_headers,
                ping_interval=None, open_timeout=10,
            )
        except TypeError:
            soop_ws_ctx = websockets.connect(
                soop_ws_url, subprotocols=["chat"], extra_headers=ws_headers,
                ping_interval=None, open_timeout=10,
            )

        async with soop_ws_ctx as soop_ws:
            logging.info(f"[SOOP WS] room={room_id} 已连接到 SOOP chat 服务器，开始发送握手包")
            await soop_ws.send(_soop_connect_packet(auth_ticket))
            await asyncio.sleep(0.2)
            await soop_ws.send(_soop_join_packet(chat_no, ftk=ftk if auth_ticket else "", _au=au, log_meta=log_meta))
            logging.info(f"[SOOP WS] room={room_id} 握手包+加入包已发送（{'认证' if auth_ticket else '匿名'}模式）")

            async def heartbeat():
                try:
                    while True:
                        await asyncio.sleep(20)
                        await soop_ws.send(_soop_heartbeat_packet())
                except Exception as e:
                    logging.warning(f"[SOOP WS] room={room_id} heartbeat 异常 [{type(e).__name__}]: {e}")

            _frame_count = 0
            _chat_count = 0

            async def forward_to_frontend():
                nonlocal _frame_count, _chat_count
                try:
                    async for msg in soop_ws:
                        if isinstance(msg, str):
                            logging.info(f"[SOOP WS] room={room_id} 收到文本帧(忽略): {msg[:200]!r}")
                            continue
                        _frame_count += 1
                        for service, body in _soop_parse_frames(msg):
                            if service == 2 and auth_ticket:
                                parts2 = body.split(_SOOP_F)
                                syn_ack = parts2[7].decode('utf-8', errors='ignore').strip() if len(parts2) > 7 else ''
                                if syn_ack:
                                    try:
                                        await soop_ws.send(_soop_enter_info_packet(syn_ack))
                                    except Exception:
                                        pass
                                continue
                            if service != 5:
                                continue
                            decoded = _soop_decode_chat(body)
                            if not decoded:
                                continue
                            _chat_count += 1
                            nick, comment = decoded
                            try:
                                await ws_conn.send_json({"type": "chat", "nick": nick, "content": comment})
                            except Exception:
                                return
                    close_code = getattr(soop_ws, "close_code", None)
                    close_reason = getattr(soop_ws, "close_reason", None)
                    logging.warning(
                        f"[SOOP WS] room={room_id} 连接被对端关闭（正常结束，无异常）。"
                        f"close_code={close_code} close_reason={close_reason!r} "
                        f"共收到二进制帧={_frame_count} 解析出弹幕={_chat_count}"
                    )
                except Exception as e:
                    logging.warning(f"[SOOP WS] room={room_id} forward_to_frontend 异常 [{type(e).__name__}]: {e}")

            async def listen_to_frontend():
                try:
                    while True:
                        data = await ws_conn.receive_text()
                        if data == "ping":
                            try:
                                await ws_conn.send_text("pong")
                            except Exception:
                                break
                except WebSocketDisconnect:
                    pass
                except Exception as e:
                    logging.debug(f"[SOOP WS] listen_to_frontend 异常: {e}")

            task_hb = asyncio.create_task(heartbeat())
            task_fwd = asyncio.create_task(forward_to_frontend())
            task_lsn = asyncio.create_task(listen_to_frontend())
            done, pending = await asyncio.wait(
                [task_hb, task_fwd, task_lsn],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    except Exception as e:
        # 【健壮性】握手/上游异常不再静默：先 send_json error 告知前端（弹幕区显示
        # 原因），再 close(1011)（前端指数退避重连；原先吞成日志 + 无码 close，
        # 前端 3s 换票死循环）
        logging.warning(f"[SOOP WS] room={room_id} 连接异常 [{type(e).__name__}]: {e}")
        try:
            await ws_conn.send_json({"type": "error", "message": f"弹幕服务器连接失败：{type(e).__name__}"})
        except Exception:
            pass
        handshake_failed = True
    finally:
        try:
            await ws_conn.close(code=1011 if handshake_failed else 1000)
        except Exception:
            pass


@app.get("/")
def root():
    return {"status": "ok"}


# 【分流补齐】前端直链起播分流用的 Worker 地址下发（原版把 WORKER_BASE 硬编码在前端，
# 现在改为后端 CF_WORKER_URL 统一配置）：前端 routeDirectStream 拿到后，对已知海外
# CDN 域名的 .flv/.m3u8 直链包 Worker 代理再起播（照原版 getProxyUrl）；未配置时前端退化为直连
@app.get("/api/config")
async def api_config():
    return {"status": "ok", "worker": CF_WORKER}

# 【P0凭据泄露】公开 /health 只输出脱敏代理地址；完整明细（含原始错误文本）走
# /health/detail，需配置 HEALTH_TOKEN 并带 X-Health-Token 请求头（未配置 token 时该接口 404 关闭）
HEALTH_TOKEN = os.getenv("HEALTH_TOKEN", "").strip()

def _proxy_health_rows(masked_only: bool) -> list:
    rows = []
    for proxy, h in _PROXY_HEALTH.items():
        stream_ok = sum(v["success"] for t, v in h["by_tag"].items() if t in _STREAM_TAGS)
        stream_fail = sum(v["fail"] for t, v in h["by_tag"].items() if t in _STREAM_TAGS)
        is_bad = stream_fail > 0 and stream_fail >= stream_ok
        row = {
            "proxy": _mask_proxy_url(proxy),
            "status": "bad" if is_bad else "ok",
            "stream_success": stream_ok,
            "stream_fail": stream_fail,
        }
        if not masked_only:
            row.update({
                "last_success_ts": h.get("last_success_ts"),
                "last_fail_ts": h.get("last_fail_ts"),
                "last_error": _scrub_proxy_secrets(str(h.get("last_error") or "")),
                "by_tag": h.get("by_tag", {}),
            })
        rows.append(row)
    rows.sort(key=lambda p: p["status"] != "bad")
    return rows

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    # 【P0凭据泄露·补】connection_pool 的 key 先脱敏再输出（此前经代理的条目会把
    # http://user:password@host 原样吐给任何访问 /health 的人）
    pool_info = {_mask_pool_key(k): {"idle_sec": round(time.time() - ts)} for k, (_, ts) in CLIENT_POOL.items()}

    # 【L5·审计修复】公开 /health 不再暴露 go_danmaku 内网端点（host:port）——
    # 完整地址（含端口）移到 token 保护的 /health/detail；公开面只留存活标志。
    return {
        "status": "alive",
        "proxies": _proxy_health_rows(masked_only=True),
        "go_danmaku": {
            "available": _GO_SERVICE_AVAILABLE,
        },
        "connection_pool": pool_info,
    }

@app.get("/health/detail")
@limiter.limit("10/minute")
async def health_detail(request: Request):
    """详细健康信息（失败原因/分标签统计）：需配置 HEALTH_TOKEN 并带 X-Health-Token 头。
    【P1·补】token 比较改恒定时间（hmac.compare_digest，防逐字节时序侧信道），
    接口本身限速 10 次/分钟（按 get_real_ip 取桶），错 token 爆破也会被限速拦截。"""
    if not HEALTH_TOKEN:
        raise HTTPException(404, "health detail disabled (set HEALTH_TOKEN to enable)")
    if not hmac.compare_digest(
        request.headers.get("x-health-token", "").encode("utf-8", "ignore"),
        HEALTH_TOKEN.encode("utf-8", "ignore"),
    ):
        raise HTTPException(403, "invalid health token")
    # 【L5·审计修复】端点明细（含内网 host:port）只在此 token 保护接口下发
    return {
        "status": "alive",
        "proxies": _proxy_health_rows(masked_only=False),
        "go_danmaku": {
            "available": _GO_SERVICE_AVAILABLE,
            "endpoint": GO_DANMAKU_BASE,
        },
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
