#!/usr/bin/env python3
"""统计仓库 *.md 行数（排除 .planning/ 与 .git/）。

分目录诊断打印；仅供维护者了解体量，**不再以行数阈值硬失败**。
文档质量门禁以 scripts/qa_check.sh 的违禁词 / 断链 / 案例 / 编译为准。
参见 .planning/MEMORY.md（2026-07-19 撤销 ≥30000 行硬指标）。
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_PARTS = {".planning", ".git"}


def should_skip(path: Path) -> bool:
    return any(part in SKIP_PARTS for part in path.parts)


def top_bucket(rel: Path) -> str:
    """分目录诊断：取相对路径首段（文件则归根）。"""
    parts = rel.parts
    if len(parts) == 1:
        return "(root)"
    return parts[0]


def main() -> int:
    by_dir: dict[str, int] = defaultdict(int)
    total = 0
    for path in sorted(ROOT.rglob("*.md")):
        if should_skip(path.relative_to(ROOT)):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            print(f"warn  skip {path.relative_to(ROOT)}: {exc}", file=sys.stderr)
            continue
        n = text.count("\n")
        if not text.endswith("\n") and text:
            n += 1
        rel = path.relative_to(ROOT)
        by_dir[top_bucket(rel)] += n
        total += n

    print(f"doc_lines={total} (excl .planning/.git; diagnostic only, no min gate)")
    for bucket in sorted(by_dir, key=lambda k: (-by_dir[k], k)):
        print(f"  {bucket}: {by_dir[bucket]}")

    print(f"ok doc_lines={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
