"""P1 二轮·hasMore 真实上游翻页验收（live marker，默认跳过）。

语义与 test_live_smoke.py 一致：网络层失败 skip、解析层失败 fail。
回答两个问题（对应用户验收标准「翻到第 3 页以上确认能继续、到末尾能停」）：

  - 能继续：斗鱼/虎牙/SOOP/B站推荐流第 1-3 页非空、hasMore=True（旧公式
    12<24 / 40<24 / 60<24 提前断页）；page 参数真实生效（B站推荐是轮换流，
    页间首房间可能重复，断言改为「三页不全相同」；斗鱼/虎牙/SOOP 断言
    三页首房间互不相同）；
  - 到末尾能停：斗鱼/虎牙/SOOP 的 page=999（远超真实末页）返回空列表、
    hasMore=False（实测斗鱼推荐 40/页 p999 空、虎牙 72 页、SOOP 55 页）；
  - 两类「没有末页」的接口各自钉住设计：
    · B站推荐（getMoreRecList）是服务端无尽流——p999 仍返回 12 条 +
      hasMore=True（fetch-until-empty 对无尽流永不停，这是语义正确的）；
    · 抖音 feed 不接受翻页参数（内容每次调用随机轮换、无 cursor）——
      hasMore 恒 False（防前端无限拉同一接口造成重复卡片）。

运行：python -m pytest backend/tests -m live -q（或 -k live_paging）
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from browse_platforms import registry  # noqa: E402


def _adapter(site: str):
    adapter = registry.get(site, "")
    if adapter is None:
        pytest.skip(f"{site} 未登记")
    return adapter


@pytest.mark.live
@pytest.mark.parametrize("site", ["douyu", "huya", "soop"])
def test_live_paging_continues_past_page_3(site):
    """推荐流能翻 3 页以上：每页非空 + hasMore=True + page 参数真实生效。"""
    adapter = _adapter(site)

    async def _scenario():
        try:
            return [await adapter.get_recommend_rooms(page, 24) for page in (1, 2, 3)]
        finally:
            await adapter.close()

    try:
        pages = asyncio.run(_scenario())
    except Exception as e:  # 网络层失败 → skip
        pytest.skip(f"{site} 网络层不可达: {type(e).__name__}: {e}")

    first_ids = []
    for idx, result in enumerate(pages, start=1):
        rows = result.get("list") if isinstance(result, dict) else None
        assert isinstance(rows, list) and rows, f"{site} 第 {idx} 页拉空（解析层失败或上游变更）"
        for row in rows:
            assert row.get("roomId"), f"{site} 第 {idx} 页房间缺 roomId"
        assert result["hasMore"] is True, f"{site} 第 {idx} 页 hasMore 应为 True（旧公式在此提前断页）"
        first_ids.append(str(rows[0]["roomId"]))
    assert len(set(first_ids)) == len(first_ids), (
        f"{site} 三页首房间全部相同（{first_ids}）——page 参数疑似未被上游接受"
    )


@pytest.mark.live
def test_live_bilibili_recommend_rotating_stream():
    """B站推荐是无尽轮换流：能连续翻页（三页不全相同）且 p999 仍有数据。"""
    adapter = _adapter("bilibili")

    async def _scenario():
        try:
            pages = [await adapter.get_recommend_rooms(page, 24) for page in (1, 2, 3)]
            far = await adapter.get_recommend_rooms(999, 24)
            return pages, far
        finally:
            await adapter.close()

    try:
        pages, far = asyncio.run(_scenario())
    except Exception as e:
        pytest.skip(f"bilibili 网络层不可达: {type(e).__name__}: {e}")

    for idx, result in enumerate(pages, start=1):
        rows = result.get("list") if isinstance(result, dict) else None
        assert isinstance(rows, list) and rows, f"bilibili 第 {idx} 页拉空"
        assert result["hasMore"] is True, f"bilibili 第 {idx} 页 hasMore 应为 True（12<24 旧公式第 1 页就断）"
    # 轮换流：三页内容不全相同（全相同 = page 参数被忽略）
    id_lists = [[str(r["roomId"]) for r in p["list"]] for p in pages]
    assert not (id_lists[0] == id_lists[1] == id_lists[2]), "三页内容完全相同——page 参数疑似被忽略"
    # 无尽流：p999 仍有 12 条 + hasMore=True（fetch-until-empty 语义下永不停止，正确）
    far_rows = far.get("list") if isinstance(far, dict) else None
    assert isinstance(far_rows, list) and far_rows, "p999 应仍有数据（实测无尽流）"
    assert far["hasMore"] is True


@pytest.mark.live
@pytest.mark.parametrize("site", ["douyu", "huya", "soop"])
def test_live_far_page_terminates(site):
    """远超末页的 page 返回空 + hasMore=False：翻页到末尾能停。"""
    adapter = _adapter(site)

    async def _scenario():
        try:
            return await adapter.get_recommend_rooms(999, 24)
        finally:
            await adapter.close()

    try:
        result = asyncio.run(_scenario())
    except Exception as e:  # 网络层失败 → skip
        pytest.skip(f"{site} 网络层不可达: {type(e).__name__}: {e}")

    rows = result.get("list") if isinstance(result, dict) else None
    assert isinstance(rows, list), f"{site} list 字段应为 list，实际 {type(rows)}"
    assert rows == [], f"{site} page=999 应返回空列表（上游真实末页在 999 之前）"
    assert result["hasMore"] is False, "空页 hasMore 必须为 False"


@pytest.mark.live
def test_live_douyin_feed_page_is_decorative():
    """抖音 feed 接口不接受翻页参数：每次调用返回随机轮换的推荐（无 cursor
    契约、两次调用内容可以不同）→ hasMore 必须恒 False，否则前端会无限拉
    同一接口（内容随机+可能重复），没有任何「下一页」语义。"""
    adapter = _adapter("douyin")

    async def _scenario():
        try:
            return await adapter.get_recommend_rooms(1, 24), await adapter.get_recommend_rooms(999, 24)
        finally:
            await adapter.close()

    try:
        p1, p999 = asyncio.run(_scenario())
    except Exception as e:
        pytest.skip(f"douyin 网络层不可达: {type(e).__name__}: {e}")

    assert isinstance(p1, dict) and isinstance(p999, dict)
    rows1 = [str(r.get("roomId")) for r in (p1.get("list") or [])]
    assert rows1, "feed 第 1 页应有数据（无数据说明解析层退化）"
    # page 参数无翻页语义：p999 也返回数据（随机轮换），但 hasMore 恒 False
    assert p1["hasMore"] is False
    assert p999["hasMore"] is False, "feed 的 page 是装饰性参数，hasMore 必须恒 False（防无限重复拉取）"
