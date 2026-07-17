.PHONY: package verify test submit gen

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

# T-1-05：仅调用仓库内固定造数脚本；场景锁定 match（不接受覆盖脚本路径）
gen:
	uv run scripts/gen_vehicle_events.py --scenario match
