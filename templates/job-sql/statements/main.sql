-- 业务逻辑主体。多路输出用 StatementSet 包裹,单作业提交(e05-C2 模式)。
--
-- ══ 上线前必须确认(交付三件套,写入 PR 描述)══
-- 1. EXPLAIN 存档:是否已生成并归档到 explain/ 目录?
-- 2. TTL 声明:本查询含 Regular Join / 无窗口聚合吗?若含,TTL 设了多少、依据是什么?
-- 3. 回撤去向:本查询输出是仅追加/回撤/upsert?下游表(ddl/sink.sql)的 connector 与之匹配吗?

-- 替换点(项目方): 按需设置全局 TTL(若含 Regular Join 或无窗口聚合)
-- SET 'table.exec.state.ttl' = '1 min';

BEGIN STATEMENT SET;

INSERT INTO ads_events_summary
SELECT page, COUNT(*) AS pv
FROM ods_events_raw
GROUP BY page;

-- 替换点(项目方): 按需增加更多 INSERT,共享同一份 StatementSet 提交

END;
