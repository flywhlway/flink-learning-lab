#!/usr/bin/env python3
"""统计 interview/**/*.md 题量（PROD-04 / D-11）。

题号形态：行首 `数字.`（如 `1. …`）。
计数 <150 则 exit 1；≥150 才 exit 0。
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INTERVIEW_DIR = ROOT / "interview"
MIN_QUESTIONS = 150
QUESTION_RE = re.compile(r"^\d+\.\s+")


def count_questions() -> tuple[int, dict[str, int]]:
    total = 0
    per_file: dict[str, int] = {}
    if not INTERVIEW_DIR.is_dir():
        return 0, per_file
    for path in sorted(INTERVIEW_DIR.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        n = 0
        for line in text.splitlines():
            if QUESTION_RE.match(line):
                n += 1
        if n:
            per_file[str(path.relative_to(ROOT))] = n
            total += n
    return total, per_file


def main() -> int:
    n, per_file = count_questions()
    print(f"interview_questions={n} min={MIN_QUESTIONS}")
    for rel, c in per_file.items():
        print(f"  {rel}: {c}")
    if n < MIN_QUESTIONS:
        print(
            f"FAIL: interview 题量 {n} < {MIN_QUESTIONS}（D-11 / PROD-04）",
            file=sys.stderr,
        )
        return 1
    print(f"ok interview_questions={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
