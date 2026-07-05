-- e09-00 · 前置:加载湖仓 jar,声明 Paimon Catalog。
-- 前提:已执行 `bash scripts/fetch_lakehouse_jars.sh` 并 cp 到 docker/jobs/,
-- 或手动放置同名 jar(网络受限环境常见,脚本已给出失败提示)。
-- 在 SQL Client 内执行:sql-client.sh -f 00-setup.sql,或整段粘贴。

ADD JAR '/opt/flink/usrlib/paimon-flink-2.2.jar';
ADD JAR '/opt/flink/usrlib/paimon-s3-1.0.1.jar';

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://warehouse/paimon',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true'
);

USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;
