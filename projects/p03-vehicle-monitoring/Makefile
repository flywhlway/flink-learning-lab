.PHONY: package verify test submit gen verify-switch truncate-alerts

COMPOSE_FILE := ../../docker/docker-compose.yml
CONTROL_VERSION ?= 2
VERIFY_WAIT_SEC ?= 45

package:
	mvn -q clean package

verify:
	bash scripts/verify.sh

test:
	mvn -q test

# T-1-05：仅复制固定 p03 jar 通配，flink run -c 固定主类（不接受任意路径参数）
submit: package
	@mkdir -p ../../docker/jobs
	@JAR=$$(ls target/p03-vehicle-monitoring-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 p03-vehicle-monitoring-*.jar，请先 make package" >&2; exit 1; fi; \
	cp "$$JAR" ../../docker/jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p03.VehicleAlertJob \
		"/opt/flink/usrlib/$$JAR_NAME"
	@echo "已提交 p03 VehicleAlertJob，打开 http://localhost:8081 查看"

# 默认回归：match 别名 → match-harsh-fault；未发 control 时 PATTERN_ID=HARSH_THEN_FAULT（D-06）
gen:
	uv run scripts/gen_vehicle_events.py --scenario match

# ClickHouse 权威表清空（切换剧本前必做，避免旧 pattern_id 污染；D-10）
truncate-alerts:
	docker compose -f $(COMPOSE_FILE) exec -T clickhouse \
		clickhouse-client --user flinklab --password flinklab123 \
		--query "TRUNCATE TABLE flinklab.vehicle_alerts"
	@echo "truncated flinklab.vehicle_alerts ok"

# D-10 切换剧本：TRUNCATE → publish-control(TRIPLE_HARSH) → gen → PATTERN_ID verify
# 禁止以 Kafka 有消息作为放行条件；CH pattern_id 为唯一权威。
# CONTROL_VERSION 须单调递增（Broadcast Gate version 规则）；复跑可 make verify-switch CONTROL_VERSION=3
verify-switch: truncate-alerts
	@echo "==> publish-control activePatterns=[TRIPLE_HARSH] version=$(CONTROL_VERSION)"
	uv run scripts/gen_vehicle_events.py \
		--publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":$(CONTROL_VERSION)}'
	@echo "==> gen match-triple-harsh"
	uv run scripts/gen_vehicle_events.py --scenario match-triple-harsh
	@echo "==> 等待作业落库（最多 $(VERIFY_WAIT_SEC)s），PATTERN_ID=TRIPLE_HARSH"
	@deadline=$$(( $$(date +%s) + $(VERIFY_WAIT_SEC) )); \
	while true; do \
		if PATTERN_ID=TRIPLE_HARSH bash scripts/verify.sh; then \
			echo "verify-switch ok (PATTERN_ID=TRIPLE_HARSH)"; \
			exit 0; \
		fi; \
		now=$$(date +%s); \
		if [ $$now -ge $$deadline ]; then \
			echo "FAIL: verify-switch 超时（$(VERIFY_WAIT_SEC)s）PATTERN_ID=TRIPLE_HARSH" >&2; \
			exit 1; \
		fi; \
		sleep 3; \
	done
