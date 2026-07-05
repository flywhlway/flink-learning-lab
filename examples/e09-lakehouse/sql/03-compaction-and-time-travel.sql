-- e09-03 · Compaction 与 Time Travel:湖表的两个"时间"能力。
--
-- Compaction:LSM 结构写入产生大量小文件与多层 sorted run,
-- compaction 合并整理、削减小文件数与读放大 —— 与 03-04(RocksDB compaction)
-- 是同一物理原理在不同存储介质上的表现形式。'full-compaction' producer
-- 模式会在每次全量合并时机顺带产出完整 changelog(02 提到的第四种模式)。
--
-- Time Travel:按快照 ID 或时间戳查询表在**过去某一时刻**的样子——
-- 这是审计、错误回溯、"如果当时这样决策会怎样"分析的基础能力,
-- 传统数据仓库很难做到,湖仓格式(Paimon/Iceberg/Hudi)是标配。

USE CATALOG paimon_catalog;
USE ods;

-- 触发一次 compaction(生产上通常配置自动/定期 compaction,而非手动)
CALL sys.compact(`table` => 'ods.orders_pk');

-- 查看历史快照列表(每次 commit 产生一个快照)
SELECT * FROM orders_pk$snapshots ORDER BY snapshot_id DESC LIMIT 10;

-- Time Travel:按快照 ID 查询(把 <ID> 换成上面查到的具体值)
-- SELECT * FROM orders_pk /*+ OPTIONS('scan.snapshot-id'='<ID>') */;

-- Time Travel:按时间戳查询(毫秒级 epoch)
-- SELECT * FROM orders_pk /*+ OPTIONS('scan.timestamp-millis'='<EPOCH_MS>') */;
