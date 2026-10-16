"""pytest 共享配置（backend/tests 专用）。

本文件此前在打包环节被清成 0 字节，导致：
  - 从仓库根运行 `python -m pytest backend/tests`（CI 与 README 用的命令）时
    `import main` 找不到模块，7 个测试文件收集失败；
  - `live` 标记未注册产生 12 条 Unknown mark 警告，且默认跳过逻辑失效。

职责（三项）：
  1. 把 backend/ 加入 sys.path——测试里是 `import main` / `import browse_api`
     这类顶层导入，必须让 backend 目录本身在搜索路径上；
  2. 注册 `live` 标记（消除未知标记警告）；
  3. 收集阶段把带 live 标记的用例默认 skip（真实上游冒烟不进 CI 门禁）；
     显式 `-m live` 时按用户选择正常执行（`-m "not live"` 也不会被干扰）。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "live: 真实上游冒烟（默认跳过；显式 -m live 时才执行）"
    )


def pytest_collection_modifyitems(config, items):
    # 用户显式带了含 live 的 -m 表达式（如 `-m live`）→ 尊重选择，不注入 skip。
    # 默认（无 -m）与 `-m "not live"`（deselect 由 pytest 自己做）走下面的 skip 注入。
    markexpr = (config.option.markexpr or "").strip()
    if "live" in markexpr:
        return
    skip_live = pytest.mark.skip(reason="live 冒烟默认跳过（显式 python -m pytest backend/tests -m live 可运行）")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)
