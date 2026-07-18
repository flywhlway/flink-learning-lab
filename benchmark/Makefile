# 仓库级压测矩阵入口（PROD-01）。仅 Makefile + Python 造数驱动（D-02）；禁止 未登记 HTTP 压测工具。
.PHONY: matrix dry-run baseline help

EPS ?= 100
WARMUP_SEC ?= 45
DURATION_SEC ?= 60

help:
	@echo "targets: matrix | dry-run | baseline"
	@echo "  matrix   — D-01 裁剪全量必跑 → 写入 baseline.md"
	@echo "  dry-run  — 低负载单单元格冒烟（EPS=$(EPS)）"
	@echo "  baseline — 断言 benchmark/baseline.md 非空"

matrix:
	WARMUP_SEC=$(WARMUP_SEC) DURATION_SEC=$(DURATION_SEC) bash scripts/run_matrix.sh

dry-run:
	DRY_RUN=1 WARMUP_SEC=15 DURATION_SEC=20 CELL_LIMIT=1 \
		bash scripts/run_matrix.sh --dry-run --eps $(EPS)

baseline:
	@test -s baseline.md || { echo "FAIL: baseline.md 不存在或为空" >&2; exit 1; }
	@echo "ok baseline.md present"
