#!/usr/bin/env python3
"""统计仓库 *.md 行数（排除 .planning/ 与 .git/）（QA-02 / D-04）。

分目录诊断打印；合计 < 30000 则 exit 1。
硬门禁仍以 scripts/qa_check.sh ⑤ 段为准；本脚本供维护者定位缺口。
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIN_LINES = 30000
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

    print(f"doc_lines={total} min={MIN_LINES} (excl .planning/.git)")
    for bucket in sorted(by_dir, key=lambda k: (-by_dir[k], k)):
        print(f"  {bucket}: {by_dir[bucket]}")

    if total < MIN_LINES:
        print(
            f"FAIL: doc_lines {total} < {MIN_LINES}（D-04 / D-06）",
            file=sys.stderr,
        )
        return 1
    print(f"ok doc_lines={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
