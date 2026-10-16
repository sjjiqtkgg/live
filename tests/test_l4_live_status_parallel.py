"""L4·审计回归：关注状态批量查询并发化（huya/douyu/bilibili 适配器）。

原实现逐房间串行：30 个房间最坏等于 30 倍单房延迟（可达数分钟）。
修复：信号量限流并发（5 路）。本测试钉住三个契约：
  ① 返回顺序与入参一致（gather 保序）；
  ② 单项异常隔离（失败项跳过，不拖垮整批）；
  ③ 并发度受限（≤5，且确实并行 >1）。
另钉住 bilibili 原有的 [:30] 截断语义不变。
"""
from __future__ import annotations

import asyncio

from browse_platforms.bilibili import BilibiliAdapter
from browse_platforms.douyu import DouyuAdapter
from browse_platforms.huya import HuyaAdapter


def _install_fake_detail(adapter, delay: float = 0.005) -> dict:
    """实例属性覆盖 get_room_detail：记录在飞峰值（并发探针）；'boom' 抛异常。"""
    inflight = {"now": 0, "max": 0}

    async def fake_detail(rid: str):
        inflight["now"] += 1
        inflight["max"] = max(inflight["max"], inflight["now"])
        try:
            await asyncio.sleep(delay)
            if rid == "boom":
                raise RuntimeError("upstream down")
            return {"isLive": True, "title": f"t-{rid}", "watching": "7"}
        finally:
            inflight["now"] -= 1

    adapter.get_room_detail = fake_detail  # type: ignore[method-assign]
    return inflight


def test_huya_live_status_order_isolation_and_concurrency():
    adapter = HuyaAdapter()
    inflight = _install_fake_detail(adapter)
    out = asyncio.run(adapter.get_live_status(["11", "boom", "33", "22"]))
    assert [o["roomId"] for o in out] == ["11", "33", "22"], "异常项应隔离且顺序保持"
    assert 2 <= inflight["max"] <= 5, f"应限流并发（实测峰值 {inflight['max']}）"


def test_douyu_live_status_order_isolation_and_concurrency():
    adapter = DouyuAdapter()
    inflight = _install_fake_detail(adapter)
    out = asyncio.run(adapter.get_live_status(["a", "boom", "b"]))
    assert [o["roomId"] for o in out] == ["a", "b"]
    assert 2 <= inflight["max"] <= 5


def test_bilibili_live_status_cap_30_and_isolation():
    adapter = BilibiliAdapter()
    inflight = _install_fake_detail(adapter, delay=0.0)
    ids = [str(i) for i in range(40)]
    out = asyncio.run(adapter.get_live_status(ids))
    assert len(out) == 30, "应保持原 [:30] 截断语义"
    assert inflight["max"] <= 5, f"并发度应 ≤5（实测 {inflight['max']}）"
