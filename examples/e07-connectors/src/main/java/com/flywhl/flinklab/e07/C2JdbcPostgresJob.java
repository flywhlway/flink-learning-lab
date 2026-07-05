package com.flywhl.flinklab.e07;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e07-C2 · JDBC × PostgreSQL:落库(幂等 upsert)+ Lookup Join 维表两连。
 *
 * <p>前置(一次):
 * <pre>
 * docker compose exec postgres psql -U flinklab -d flinklab -c "
 *   CREATE TABLE IF NOT EXISTS page_pv(page VARCHAR PRIMARY KEY, pv BIGINT);
 *   CREATE TABLE IF NOT EXISTS dim_page(page VARCHAR PRIMARY KEY, owner VARCHAR);
 *   INSERT INTO dim_page VALUES ('a','团队甲'),('b','团队乙') ON CONFLICT DO NOTHING;"
 * </pre>
 * 语义要点:① DDL 声明 PRIMARY KEY ⇒ JDBC sink 走 upsert(INSERT ... ON CONFLICT),
 * 回撤流被吸收成幂等覆盖 —— 这就是"方案 A:幂等"的 SQL 形态(docs/04-04);
 * ② Lookup Join 用 FOR SYSTEM_TIME AS OF proc_time,点查 PG 并按
 * lookup.cache 参数缓存 —— 维度更新可见性 = 缓存 TTL(与 e11-C3 同一权衡)。
 *
 * <p>提交(集群):flink run -d -c 本类 e07-connectors jar;
 * 验证:psql 查询 page_pv 行随时间增大且无重复主键。
 */
public final class C2JdbcPostgresJob {
    private C2JdbcPostgresJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE clicks (
                    page STRING, proc_time AS PROCTIME()
                ) WITH ('connector'='datagen','rows-per-second'='10',
                        'fields.page.length'='1')""");

        t.executeSql("""
                CREATE TABLE pg_pv (
                    page STRING, pv BIGINT, PRIMARY KEY (page) NOT ENFORCED
                ) WITH ('connector'='jdbc',
                        'url'='jdbc:postgresql://postgres:5432/flinklab',
                        'table-name'='page_pv',
                        'username'='flinklab','password'='flinklab123')""");

        t.executeSql("""
                CREATE TABLE pg_dim (
                    page STRING, owner STRING
                ) WITH ('connector'='jdbc',
                        'url'='jdbc:postgresql://postgres:5432/flinklab',
                        'table-name'='dim_page',
                        'username'='flinklab','password'='flinklab123',
                        'lookup.cache'='PARTIAL',
                        'lookup.partial-cache.max-rows'='1000',
                        'lookup.partial-cache.expire-after-write'='1 min')""");

        t.executeSql("""
                CREATE TABLE enriched_print (page STRING, owner STRING)
                WITH ('connector'='print')""");

        var set = t.createStatementSet();
        set.addInsertSql("INSERT INTO pg_pv SELECT page, COUNT(*) FROM clicks GROUP BY page");
        set.addInsertSql("""
                INSERT INTO enriched_print
                SELECT c.page, COALESCE(d.owner, '未认领')
                FROM clicks c
                LEFT JOIN pg_dim FOR SYSTEM_TIME AS OF c.proc_time AS d
                  ON c.page = d.page""");
        set.execute().await();
    }
}
