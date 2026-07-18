# p03 VehicleAlertJob Application 镜像（OrbStack 本地，非 :latest）
# 基镜像：flink:2.2.1-java21（根 README 版本矩阵）
FROM flink:2.2.1-java21

# 启用内置 S3 插件（checkpoint/savepoint → compose MinIO）
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.2.1.jar

# 构建上下文：仓库根目录
# docker build -f production/docker/p03-k8s-image/Dockerfile -t flinklab/p03-vehicle-alert:dev .
# 仅 shade jar（排除 original-*）；版本与 projects/p03-vehicle-monitoring/pom.xml 对齐
COPY projects/p03-vehicle-monitoring/target/p03-vehicle-monitoring-0.1.0.jar /opt/flink/usrlib/p03-vehicle-monitoring.jar
