# 仓库级压测矩阵入口（PROD-01）。仅 Makefile + Python 造数驱动（D-02）。
.PHONY: matrix dry-run baseline help

EPS ?= 1000
DRY_RUN ?= 0

help:
	@echo "targets: matrix | dry-run | baseline"
	@echo "  matrix   — 跑完整矩阵（Wave 0 默认 FAIL，需 --implemented）"
	@echo "  dry-run  — 低负载冒烟声明（仍 FAIL 直至矩阵实现）"
	@echo "  baseline — 断言 benchmark/baseline.md 非空"

matrix:
	bash scripts/run_matrix.sh --eps $(EPS)

dry-run:
	DRY_RUN=1 bash scripts/run_matrix.sh --dry-run --eps $(EPS)

baseline:
	@test -s baseline.md || { echo "FAIL: baseline.md 不存在或为空" >&2; exit 1; }
	@echo "ok baseline.md present"
