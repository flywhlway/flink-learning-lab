.PHONY: package test submit submit-ai cancel-p01 gen gen-ai verify verify-ai truncate-results match match-ai

COMPOSE_FILE := ../../docker/docker-compose.yml
VERIFY_WAIT_SEC ?= 60
# 本机 Ollama 模型名；宿主机若无 qwen3:8b 可覆盖，例如 make submit-ai AI_MODEL=qwen3.5:9b-mlx
AI_MODEL ?= qwen3.5:9b-mlx
AI_TIMEOUT_MS ?= 30000
VERIFY_AI_WAIT_SEC ?= 120

package:
	mvn -q clean package

test:
	mvn -q -Dtest=BudgetGateTest,RuleTaggerTest,ParseLogJsonTest test

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

# 可选 AI 轨：取消同名作业后以 --ai.enabled=true 提交（D-06）
# 模型名默认 AI_MODEL（本机常见 qwen3.5:9b-mlx）；可用 make submit-ai AI_MODEL=... 覆盖
submit-ai: package
	@mkdir -p ../../docker/jobs
	@JAR=$$(ls target/p01-log-ai-platform-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 p01-log-ai-platform-*.jar，请先 make package" >&2; exit 1; fi; \
	cp "$$JAR" ../../docker/jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	$(MAKE) cancel-p01 || true; \
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p01.LogAiJob \
		"/opt/flink/usrlib/$$JAR_NAME" \
		--ai.enabled=true \
		--ai.model=$(AI_MODEL) \
		--ai.timeout-ms=$(AI_TIMEOUT_MS) \
		--ai.endpoint=http://host.docker.internal:11434 \
		--group-id=p01-log-ai-verify
	@echo "已提交 p01 LogAiJob（--ai.enabled=true --ai.model=$(AI_MODEL)），打开 http://localhost:8081 查看"

# 取消名称含 p01-log-ai 的 RUNNING/RESTARTING 作业（幂等）
cancel-p01:
	@cd ../../docker && \
	IDS=$$(docker compose exec -T jobmanager flink list -r 2>/dev/null \
		| awk -F' : ' '/p01-log-ai/ {print $$2}' | awk '{print $$1}' || true); \
	if [ -z "$$IDS" ]; then echo "cancel-p01: 无匹配作业"; exit 0; fi; \
	for id in $$IDS; do \
		echo "cancel $$id"; \
		docker compose exec -T jobmanager flink cancel "$$id" || true; \
	done

gen:
	uv run scripts/gen_log_events.py --scenario rule-auth-fail

gen-ai:
	uv run scripts/gen_log_events.py --scenario ai-risk-high

verify:
	bash scripts/verify.sh

# 可选 AI 验收轨（D-06）：前置 Ollama；CH 断言 ai_source=AI
# 一键：submit-ai → truncate → gen-ai → 轮询 verify_ai.sh
verify-ai:
	@echo "==> verify-ai：前置 Ollama + 提交 AI 作业 + ai-risk-high 造数 + CH 断言"
	@if ! curl -sf --max-time 5 http://127.0.0.1:11434/api/tags >/dev/null; then \
		echo "FAIL: Ollama 不可达（http://127.0.0.1:11434/api/tags）— 非默认门禁" >&2; \
		echo "hint: ollama serve && ollama list；或 make submit-ai AI_MODEL=<本机模型>" >&2; \
		exit 1; \
	fi
	$(MAKE) submit-ai
	@sleep 8
	$(MAKE) truncate-results
	$(MAKE) gen-ai
	@echo "==> 等待 AI 落库（最多 $(VERIFY_AI_WAIT_SEC)s）ai_source=AI"
	@deadline=$$(( $$(date +%s) + $(VERIFY_AI_WAIT_SEC) )); \
	while true; do \
		if bash scripts/verify_ai.sh; then \
			echo "verify-ai ok"; \
			exit 0; \
		fi; \
		now=$$(date +%s); \
		if [ $$now -ge $$deadline ]; then \
			echo "FAIL: verify-ai 超时（$(VERIFY_AI_WAIT_SEC)s）" >&2; \
			exit 1; \
		fi; \
		sleep 5; \
	done

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

# AI 轨一键（假定作业已 submit-ai RUNNING）
match-ai: truncate-results gen-ai
	@echo "==> 等待 AI 落库（最多 $(VERIFY_AI_WAIT_SEC)s）"
	@deadline=$$(( $$(date +%s) + $(VERIFY_AI_WAIT_SEC) )); \
	while true; do \
		if bash scripts/verify_ai.sh; then \
			echo "match-ai ok"; \
			exit 0; \
		fi; \
		now=$$(date +%s); \
		if [ $$now -ge $$deadline ]; then \
			echo "FAIL: match-ai 超时（$(VERIFY_AI_WAIT_SEC)s）" >&2; \
			exit 1; \
		fi; \
		sleep 5; \
	done
