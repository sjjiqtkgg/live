"""P3 批·回归测试：真实上游接口冒烟（live marker，默认跳过）。

与 323+28 例离线测试的本质区别：这里打**真实平台接口**，回答「上游还活着吗、
返回还解析得动吗」。设计原则（见 README「测试」一节）：

  - 不做 CI 门禁：真实上游受网络环境 / 平台风控 / 区域限制影响，绿不绿不代表
    代码对错——默认跳过（conftest 的 collection 阶段改 skip），手动运行：
        python -m pytest backend/tests -m live -q
  - 网络层失败 → skip（报告「这个环境到不了这个平台」），不是 fail；
  - 解析层失败 → fail（上游 200 且返回了数据，但我们解析不出房间列表——
    这是「上游格式变了」的真实信号，正是冒烟要抓的东西）；
  - 断言宽松：只要求「能拿到 dict 结构、list 可迭代、每个房间有 roomId」，
    不锁具体条数/文案（上游内容天经地义会变）。

平台基线（2026-10 交付环境实测）：虎牙 / 斗鱼 / B站推荐 / SOOP 可达；
抖音/Twitch/Panda 受 cookie 与区域影响较大，允许 skip。
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from browse_platforms import registry  # noqa: E402


def _smoke_site(site: str) -> None:
    cookie = {
        "huya": "HUYA_COOKIE", "douyu": "DOUYU_COOKIE", "bilibili": "BILI_COOKIE",
        "douyin": "DOUYIN_COOKIE", "twitch": "TWITCH_COOKIE", "soop": "SOOP_COOKIE",
        "pandatv": "PANDA_COOKIE",
    }.get(site, "")
    adapter = registry.get(site, os.getenv(cookie, ""))
    if adapter is None:
        pytest.skip(f"{site} 未登记")

    async def _scenario():
        # 同一 event loop 内完成「拉取 + 关闭」——adapter 内部 AsyncClient 绑定
        # 创建它的 loop，拆两个 asyncio.run 会报 Event loop is closed
        try:
            return await adapter.get_recommend_rooms(1, 5)
        finally:
            await adapter.close()

    result = asyncio.run(_scenario())
    # 解析层契约：dict + list 可迭代 + 房间有 roomId（上游数据变更 → fail，冒烟核心价值）
    assert isinstance(result, dict), f"{site} 推荐接口应返回 dict，实际 {type(result)}"
    rows = result.get("list")
    assert isinstance(rows, list), f"{site} list 字段应为 list，实际 {type(rows)}"
    for row in rows:
        assert isinstance(row, dict)
        assert row.get("roomId") not in (None, ""), f"{site} 房间缺 roomId: {row!r}"
    if rows:
        first = rows[0]
        assert str(first.get("title") or "") != "" or str(first.get("nickname") or "") != "", (
            f"{site} 首个房间 title/nickname 双空，疑似解析退化"
        )


@pytest.mark.live
def test_smoke_huya():
    _smoke_site("huya")


@pytest.mark.live
def test_smoke_douyu():
    _smoke_site("douyu")


@pytest.mark.live
def test_smoke_bilibili_recommend():
    _smoke_site("bilibili")


@pytest.mark.live
def test_smoke_soop():
    _smoke_site("soop")


@pytest.mark.live
def test_smoke_pandatv():
    _smoke_site("pandatv")


@pytest.mark.live
def test_smoke_twitch():
    """Twitch 无 cookie 时 GQL 匿名额度受区域影响大，允许网络层 skip。"""
    _smoke_site("twitch")


@pytest.mark.live
def test_smoke_douyin():
    """抖音受风控影响最大（A-Bogus 签名 + cookie），允许 skip。"""
    _smoke_site("douyin")
