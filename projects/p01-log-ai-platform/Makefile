.PHONY: package test submit gen verify truncate-results match

COMPOSE_FILE := ../../docker/docker-compose.yml
VERIFY_WAIT_SEC ?= 60

package:
	mvn -q clean package

test:
	mvn -q -Dtest=RuleTaggerTest,ParseLogJsonTest test

# 文档约定：package → submit（ai off）→ gen → verify（CH 权威）
# 仅复制固定 p01 jar 通配，flink run -c 固定主类；默认 AI off
submit: package
	@mkdir -p ../../docker/jobs
	@JAR=$$(ls target/p01-log-ai-platform-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 p01-log-ai-platform-*.jar，请先 make package" >&2; exit 1; fi; \
	cp "$$JAR" ../../docker/jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p01.LogAiJob \
		"/opt/flink/usrlib/$$JAR_NAME" \
		--ai.enabled=false
	@echo "已提交 p01 LogAiJob（--ai.enabled=false），打开 http://localhost:8081 查看"

gen:
	uv run scripts/gen_log_events.py --scenario rule-auth-fail

verify:
	bash scripts/verify.sh

truncate-results:
	docker compose -f $(COMPOSE_FILE) exec -T clickhouse \
		clickhouse-client --user flinklab --password flinklab123 \
		--query "TRUNCATE TABLE flinklab.log_results"
	@echo "truncated flinklab.log_results ok"

# 一键：清空 → 造数 → 等待 CH 断言
match: truncate-results gen
	@echo "==> 等待作业落库（最多 $(VERIFY_WAIT_SEC)s），RULE_LABEL=AUTH_FAIL"
	@deadline=$$(( $$(date +%s) + $(VERIFY_WAIT_SEC) )); \
	while true; do \
		if RULE_LABEL=AUTH_FAIL bash scripts/verify.sh; then \
			echo "match ok (RULE_LABEL=AUTH_FAIL)"; \
			exit 0; \
		fi; \
		now=$$(date +%s); \
		if [ $$now -ge $$deadline ]; then \
			echo "FAIL: match 超时（$(VERIFY_WAIT_SEC)s）RULE_LABEL=AUTH_FAIL" >&2; \
			exit 1; \
		fi; \
		sleep 3; \
	done
