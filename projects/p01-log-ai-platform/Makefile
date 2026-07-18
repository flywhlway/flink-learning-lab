.PHONY: package test submit

COMPOSE_FILE := ../../docker/docker-compose.yml

package:
	mvn -q clean package

test:
	mvn -q -Dtest=ParseLogJsonTest test

# 仅复制固定 p01 jar 通配，flink run -c 固定主类；默认 AI off（JobConfig / 显式参数）
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
