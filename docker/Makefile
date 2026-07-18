# flink-learning-lab · docker 环境入口
SHELL := /bin/bash
TM ?= 2            # TaskManager 数量(2 台 × 4 slot = 8 slots)

.PHONY: up up-ai up-p03 up-p01 up-p02 down clean ps logs init urls topics submit-e01 submit-p03 submit-p03-window submit-p01 sql savepoint-e01

up:            ## 启动全部核心服务(TaskManager 默认 2 个,不含 AI/p03 profile)
	docker compose up -d --scale taskmanager=$(TM)
	@echo "等待核心服务健康……" && sleep 3 && docker compose ps

up-ai:         ## 额外启动 Phase 3 AI profile(Milvus 向量库;核心服务需已 up)
	docker compose --profile ai up -d milvus-etcd milvus-minio milvus-standalone
	@echo "Milvus 启动中,等待健康检查……" && sleep 3 && docker compose --profile ai ps

up-p03:        ## 额外启动 p03 profile(创建 vehicle topics + CH DDL;核心服务需已 up 或一并拉起依赖)
	docker compose --profile p03 up -d p03-init
	@echo "p03-init 已调度,查看: docker compose --profile p03 ps -a"

up-p01:        ## 额外启动 p01 profile(创建 logs.events + CH log_results DDL;核心服务需已 up 或一并拉起依赖)
	docker compose --profile p01 up -d p01-init
	@echo "p01-init 已调度,查看: docker compose --profile p01 ps -a"

up-p02:        ## 额外启动 p02 profile(reco topics + CH reco_results + PG reco_items 种子;核心服务需已 up)
	@echo "提示: 请确保 redis / postgres 已在 default make up 中可达（p02 不新建这两个服务）"
	docker compose --profile p02 up -d p02-init
	@echo "==> up-p02: 种子 PostgreSQL reco_items（psql -f；非 CH HTTP）"
	docker compose exec -T postgres \
		psql -U $${PG_USER:-flinklab} -d flinklab \
		-v ON_ERROR_STOP=1 \
		-f - < ../projects/p02-realtime-reco/sql/postgres_reco_items.sql
	@echo "p02-init 已调度且 reco_items 已种子,查看: docker compose --profile p02 ps -a"

down:          ## 停止(保留数据卷)
	docker compose down

clean:         ## 停止并清空全部数据卷(危险)
	docker compose down -v

ps:
	docker compose ps

logs:          ## 用法: make logs S=jobmanager
	docker compose logs -f $(S)

init:          ## 初始化 topic / bucket / ClickHouse(幂等)
	bash ./init.sh

topics:        ## 列出 Kafka topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

urls:
	@echo "Flink Dashboard   http://localhost:8081"
	@echo "Kafka UI          http://localhost:8080"
	@echo "Grafana           http://localhost:3000   (admin / flinklab)"
	@echo "Prometheus        http://localhost:9090"
	@echo "MinIO Console     http://localhost:9001   (flinklab / flinklab123)"
	@echo "ClickHouse HTTP   http://localhost:8123   (flinklab / flinklab123)"
	@echo "Kafka(宿主机)   localhost:9094"
	@echo "PostgreSQL        localhost:5432          (flinklab / flinklab123)"
	@echo "Redis             localhost:6379"
	@echo "Milvus(profile=ai,先 make up-ai)  localhost:19530  WebUI http://localhost:9091/webui/"
	@echo "Ollama(宿主机原生,非容器)         http://localhost:11434(容器内访问用 http://host.docker.internal:11434)"

sql:           ## 打开 Flink SQL Client(交互式)
	docker compose exec -it jobmanager ./bin/sql-client.sh

submit-e01:    ## 提交 e01 的 Kafka 端到端作业(先在 examples/ 下 mvn package)
	cp ../examples/e01-hello-flink/target/e01-hello-flink-*.jar ./jobs/ 2>/dev/null || \
		(echo "未找到 jar,请先: cd ../examples && mvn -q clean package" && exit 1)
	docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.e01.KafkaClickstreamWindowJob \
		/opt/flink/usrlib/$$(ls ../examples/e01-hello-flink/target | grep '^e01-hello-flink.*\.jar$$' | head -1)
	@echo "已提交,打开 http://localhost:8081 查看"

submit-p03:    ## 提交 p03 车联网告警作业(先在 projects/p03-vehicle-monitoring 下 mvn package)
	@JAR=$$(ls ../projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 jar,请先: cd ../projects/p03-vehicle-monitoring && mvn -q clean package" && exit 1; fi; \
	mkdir -p ./jobs && cp "$$JAR" ./jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p03.VehicleAlertJob \
		"/opt/flink/usrlib/$$JAR_NAME"
	@echo "已提交 p03,打开 http://localhost:8081 查看"

submit-p03-window: ## 提交 p03 旁路窗口指标作业(同 jar，主类 VehicleWindowMetricsJob)
	@JAR=$$(ls ../projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 jar,请先: cd ../projects/p03-vehicle-monitoring && mvn -q clean package" && exit 1; fi; \
	mkdir -p ./jobs && cp "$$JAR" ./jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p03.VehicleWindowMetricsJob \
		"/opt/flink/usrlib/$$JAR_NAME"
	@echo "已提交 p03 VehicleWindowMetricsJob,打开 http://localhost:8081 查看"

submit-p01:    ## 提交 p01 日志 AI 作业(先在 projects/p01-log-ai-platform 下 mvn package；默认 AI off)
	@JAR=$$(ls ../projects/p01-log-ai-platform/target/p01-log-ai-platform-*.jar 2>/dev/null | grep -v original | head -1); \
	if [ -z "$$JAR" ]; then echo "未找到 jar,请先: cd ../projects/p01-log-ai-platform && mvn -q clean package" && exit 1; fi; \
	mkdir -p ./jobs && cp "$$JAR" ./jobs/; \
	JAR_NAME=$$(basename "$$JAR"); \
	docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p01.LogAiJob \
		"/opt/flink/usrlib/$$JAR_NAME" \
		--ai.enabled=false
	@echo "已提交 p01 LogAiJob（--ai.enabled=false）,打开 http://localhost:8081 查看"

savepoint-e01: ## 对指定作业触发 savepoint: make savepoint-e01 JOB=<jobId>
	docker compose exec jobmanager flink savepoint $(JOB) s3://flink/savepoints
