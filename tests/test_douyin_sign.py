"""抖音 a_bogus 签名异步化单测（backend/browse_platforms/douyin.py）。

背景（本批修复）：_ABogus.sign 走 execjs.call / subprocess.run（起 node 子进程，
冷启动数百 ms 起、timeout 20s），此前在 async 函数里被【同步】调用 —— 阻塞整个
asyncio 事件循环，期间所有并发请求与弹幕 WS 转发全部卡死。修复后 sign 为
async（asyncio.to_thread 执行 _sign_sync），_signed_url 改 async。

本文件零第三方异步插件依赖（无 pytest-asyncio）：async 用例统一 asyncio.run 包装。
真签名用例依赖 PATH 里有 node（CI backend job 已 setup-node；本地缺失自动 skip）。
"""
import asyncio
import inspect
import shutil
import time

import pytest

from browse_platforms.douyin import _ABogus, _signed_url

HAS_NODE = shutil.which("node") is not None
needs_node = pytest.mark.skipif(not HAS_NODE, reason="PATH 无 node，跳过真签名用例")


# ==================== 接口形态：同步阻塞已移出事件循环 ====================

def test_sign_is_coroutine_function():
    assert inspect.iscoroutinefunction(_ABogus.sign)


def test_signed_url_is_coroutine_function():
    assert inspect.iscoroutinefunction(_signed_url)


def test_sign_sync_still_callable_synchronously():
    """线程池里跑的同步实现保留（锁 + 懒编译）。"""
    assert callable(_ABogus._sign_sync)
    assert _ABogus._sign_lock is not None


# ==================== 签名不阻塞事件循环（本批核心行为） ====================

@needs_node
def test_sign_keeps_event_loop_responsive():
    """签名（子进程，数百 ms）期间事件循环必须仍能调度其它协程；
    修复前同步阻塞时 ticker 一个 tick 都跑不动。"""

    async def _run():
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.005)

        task = asyncio.create_task(ticker())
        try:
            t0 = time.monotonic()
            sig = await _ABogus.sign("aid=6383&app_name=douyin_web")
            dt = time.monotonic() - t0
        finally:
            task.cancel()
        assert sig, "签名为空"
        assert ticks >= 3, (
            f"签名期间事件循环被阻塞：ticks={ticks}（应持续跳秒），签名耗时 {dt:.2f}s"
        )

    asyncio.run(_run())


@needs_node
def test_concurrent_signs_all_succeed():
    """并发签名（线程池 + 串行锁）不竞态、不丢结果。"""

    async def _run():
        queries = [f"aid=6383&page={i}" for i in range(4)]
        sigs = await asyncio.gather(*(_ABogus.sign(q) for q in queries))
        assert all(s and isinstance(s, str) for s in sigs), sigs

    asyncio.run(_run())


# ==================== 真签名形态与 URL 组装 ====================

@needs_node
def test_sign_sync_real_signature_shape():
    sig = _ABogus._sign_sync("aid=6383&app_name=douyin_web")
    assert isinstance(sig, str) and len(sig) >= 64, f"a_bogus 签名形态异常: {sig!r}"
    # 上游 getABogus 输出为 URL-safe base64 风格字符集（实测 128 字符）
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=/+")
    assert set(sig) <= allowed, f"签名含非法字符: {sig!r}"


@needs_node
def test_signed_url_composition():
    """msToken 缺失时自动生成；query 顺序保留；a_bogus 挂尾。"""

    async def _run():
        url = await _signed_url("https://live.douyin.com/webcast/x/", {"aid": "6383", "count": "10"}, "")
        assert url.startswith("https://live.douyin.com/webcast/x/?aid=6383&count=10&msToken=")
        assert "&a_bogus=" in url
        tail = url.rsplit("&a_bogus=", 1)[1]
        assert tail, "a_bogus 为空"

    asyncio.run(_run())


@needs_node
def test_signed_url_uses_cookie_ms_token():
    """cookie 里带 msToken 时直接复用（不重新生成随机值）。"""

    async def _run():
        cookie = "ttwid=abc; msToken=fixedtoken123; other=1"
        url = await _signed_url("https://x/", {"aid": "6383"}, cookie)
        assert "msToken=fixedtoken123" in url

    asyncio.run(_run())
