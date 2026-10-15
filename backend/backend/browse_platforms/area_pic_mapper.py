"""无图分类的图片回填，照 pure_live area_pic_mapper.dart 移植。

虎牙/斗鱼/B站的分类带官方图标，抖音的 categoryData 只有 id_str/type/title
没有图。本模块把带图平台的「分区名 → 图标 URL」收集成映射，无图平台按
精确 → 别名 → 相互包含 → 模糊AND → 别名包含 → 模糊OR 的顺序回查，
匹配算法（含 dice 系数）与 pure_live 一致。
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Any

# 照 pure_live _hardcodedAliases：各平台对同类分区的不同叫法
_HARDCODED_ALIASES = {
    "射击游戏": "穿越火线",
    "fps": "穿越火线",
    "cs2": "穿越火线",
    "csgo": "穿越火线",
    "cf": "穿越火线",

    "网游竞技": "英雄联盟",
    "竞技游戏": "英雄联盟",
    "moba": "英雄联盟",
    "dota2": "英雄联盟",

    "休闲益智": "互动组队",
    "文化": "成年教育",

    "娱乐生活": "户外",
    "交友娱乐": "户外",
    "吃喝玩乐": "户外",

    "角色扮演": "原神",
    "rpg": "原神",
    "mmo": "原神",

    "策略卡牌": "金铲铲之战",
    "棋牌策略": "金铲铲之战",
}

_TOKEN_SPLIT = re.compile(r"[\s,;|：:_—\-]+")


def build_pic_map(sources: list[list[dict[str, Any]]]) -> dict[str, str]:
    """照 pure_live updateAreaListMaps：收集各平台子分区的 名称→图标。"""
    pic_map: dict[str, str] = {}
    for categories in sources:
        for group in categories or []:
            if not isinstance(group, dict):
                continue
            for area in (group.get("children") or []):
                if not isinstance(area, dict):
                    continue
                name = str(area.get("name") or "").strip()
                pic = str(area.get("pic") or "").strip()
                if name and pic:
                    pic_map[name] = pic
    return pic_map


def _dice(a: str, b: str) -> float:
    # string_similarity 包的 compareTwoStrings（bigram dice 系数）
    if len(a) < 2 or len(b) < 2:
        return 0.0
    bigrams_a = [a[i:i + 2] for i in range(len(a) - 1)]
    bigrams_b = [b[i:i + 2] for i in range(len(b) - 1)]
    counter_a, counter_b = Counter(bigrams_a), Counter(bigrams_b)
    intersection = sum((counter_a & counter_b).values())
    return 2.0 * intersection / (len(bigrams_a) + len(bigrams_b))


def _tokens(query: str) -> list[str]:
    return [t for t in _TOKEN_SPLIT.split(query.lower()) if len(t) > 1]


def _fuzzy_and(query: str, blob: str) -> float:
    # 照 pure_live _fuzzyMatchScoreAnd：所有 token 都要命中
    tokens = _tokens(query)
    if not tokens:
        return 0.0
    blob_lower = blob.lower()
    words = [w for w in _TOKEN_SPLIT.split(blob_lower) if w]
    score = 0.0
    for token in tokens:
        if token in blob_lower:
            score += 1.0
            continue
        best = 0.0
        for word in words:
            sim = _dice(token, word)
            if sim > best:
                best = sim
        if best > 0.6:
            score += 0.5
        else:
            return 0.0
    return score


def _fuzzy_or(query: str, blob: str) -> float:
    # 照 pure_live _fuzzyMatchScoreOr：任一 token 命中即可
    tokens = _tokens(query)
    if not tokens:
        return 0.0
    blob_lower = blob.lower()
    words = [w for w in _TOKEN_SPLIT.split(blob_lower) if w]
    total = 0.0
    any_match = False
    for token in tokens:
        if token in blob_lower:
            total += 1.0
            any_match = True
            continue
        best = 0.0
        for word in words:
            sim = _dice(token, word)
            if sim > best:
                best = sim
        if best > 0.6:
            total += 0.5
            any_match = True
    return total if any_match else 0.0


def get_pic(area_name: str, pic_map: dict[str, str]) -> str:
    """照 pure_live AreaPicMapper.getPic 的六级回查顺序。"""
    clean = area_name.strip().lower()
    if not clean:
        return ""

    for key, pic in pic_map.items():
        if key.strip().lower() == clean:
            return pic

    if clean in _HARDCODED_ALIASES:
        target = _HARDCODED_ALIASES[clean].strip().lower()
        for key, pic in pic_map.items():
            if key.strip().lower() == target:
                return pic

    for key, pic in pic_map.items():
        low = key.strip().lower()
        if low and pic and (low in clean or clean in low):
            return pic

    best_key, best_score = "", 0.0
    for key in pic_map:
        score = _fuzzy_and(clean, key)
        if score > best_score:
            best_score, best_key = score, key
    if best_score > 0.0 and best_key:
        return pic_map[best_key]

    for alias, target in _HARDCODED_ALIASES.items():
        if alias in clean:
            target_lower = target.strip().lower()
            for key, pic in pic_map.items():
                if key.strip().lower() == target_lower:
                    return pic

    best_key, best_score = "", 0.0
    for key in pic_map:
        score = _fuzzy_or(clean, key)
        if score > best_score:
            best_score, best_key = score, key
    if best_score > 0.0 and best_key:
        return pic_map[best_key]

    return ""
