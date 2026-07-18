.PHONY: package test submit cancel-p02 gen verify truncate-results match loadtest drill-redis

COMPOSE_FILE := ../../docker/docker-compose.yml
VERIFY_WAIT_SEC ?= 60

package:
	mvn -q clean package

test:
	mvn -q test

# 文档约定：package → submit → gen → verify（CH 权威）
# 仅复制固定 p02 jar 通配，flink run -c 固定主类
submit: package
	@mkdir -p ../../docker/jobs
	@JAR=$$(ls target/p02-realtime-reco-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 p02-realtime-reco-*.jar，请先 make package" >&2; exit 1; fi; \
	cp "$$JAR" ../../docker/jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p02.RealtimeRecoJob \
		"/opt/flink/usrlib/$$JAR_NAME"
	@echo "已提交 p02 RealtimeRecoJob，打开 http://localhost:8081 查看"

# 取消名称含 p02-realtime-reco 的 RUNNING/RESTARTING 作业（幂等）
cancel-p02:
	@cd ../../docker && \
	IDS=$$(docker compose exec -T jobmanager flink list -r 2>/dev/null \
		| awk -F' : ' '/p02-realtime-reco/ {print $$2}' | awk '{print $$1}' || true); \
	if [ -z "$$IDS" ]; then echo "cancel-p02: 无匹配作业"; exit 0; fi; \
	for id in $$IDS; do \
		echo "cancel $$id"; \
		docker compose exec -T jobmanager flink cancel "$$id" || true; \
	done

gen:
	uv run scripts/gen_reco_events.py --scenario feature-score

verify:
	bash scripts/verify.sh

truncate-results:
	docker compose -f $(COMPOSE_FILE) exec -T clickhouse \
		clickhouse-client --user flinklab --password flinklab123 \
		--query "TRUNCATE TABLE flinklab.reco_results"
	@echo "truncated flinklab.reco_results ok"

# 一键：清空 → 提交作业 → 造数 → 轮询 CH 权威断言（D-10）
match: truncate-results
	@$(MAKE) cancel-p02
	@$(MAKE) submit
	@echo "==> 等待作业 RUNNING（8s）"
	@sleep 8
	@$(MAKE) gen
	@echo "==> 等待作业落库（最多 $(VERIFY_WAIT_SEC)s）"
	@deadline=$$(( $$(date +%s) + $(VERIFY_WAIT_SEC) )); \
	while true; do \
		if bash scripts/verify.sh; then \
			echo "match ok"; \
			exit 0; \
		fi; \
		now=$$(date +%s); \
		if [ $$now -ge $$deadline ]; then \
			echo "FAIL: match 超时（$(VERIFY_WAIT_SEC)s）" >&2; \
			bash scripts/verify.sh || true; \
			exit 1; \
		fi; \
		sleep 3; \
	done

# RECO-03：Redis 降级演练（门闩骨架；--implemented 后绿）
drill-redis:
	bash scripts/drill_redis_degrade.sh

# RECO-03：项目级压测 → docs/baseline.md（OrbStack 实测；可用 RATE/WARMUP_SEC/DURATION_SEC 覆盖）
RATE ?= 100
WARMUP_SEC ?= 30
DURATION_SEC ?= 90

loadtest:
	RATE=$(RATE) WARMUP_SEC=$(WARMUP_SEC) DURATION_SEC=$(DURATION_SEC) bash scripts/loadtest.sh
