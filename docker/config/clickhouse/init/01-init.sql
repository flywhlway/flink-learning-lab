-- flink-learning-lab · ClickHouse 初始化(容器首次启动时自动执行)
CREATE DATABASE IF NOT EXISTS flinklab;

-- e01 演示:窗口聚合结果落库表(后续模块由 Flink JDBC/CH connector 写入)
CREATE TABLE IF NOT EXISTS flinklab.click_window_agg
(
    window_start DateTime64(3),
    window_end   DateTime64(3),
    page         String,
    clicks       UInt64,
    users        UInt64
)
ENGINE = MergeTree
ORDER BY (window_start, page);
