package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e05-C4 · 去重:每设备只保留最新一条状态(ROW_NUMBER … rn=1)。
 *
 * <p>ORDER BY 时间列 DESC + rn=1 = "keep last"(回撤型,upsert 下游);
 * ASC + rn=1 = "keep first"(可退化为仅追加,常用于幂等去重)。
 * 这是 CDC 落地、榜单快照、车辆最新状态表(案例三)统一的底层模式。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C4DedupLatestJob
 */
public final class C4DedupLatestJob {
    private C4DedupLatestJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE device_status (
                    device_id INT, temp DOUBLE, proc_time AS PROCTIME()
                ) WITH (
                    'connector'='datagen','rows-per-second'='5',
                    'fields.device_id.min'='1','fields.device_id.max'='3',
                    'fields.temp.min'='20','fields.temp.max'='90')""");

        t.executeSql("""
                CREATE TABLE latest_out (device_id INT, temp DOUBLE)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO latest_out
                SELECT device_id, temp FROM (
                  SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY device_id ORDER BY proc_time DESC) AS rn
                  FROM device_status)
                WHERE rn = 1""")
         .await();
    }
}
