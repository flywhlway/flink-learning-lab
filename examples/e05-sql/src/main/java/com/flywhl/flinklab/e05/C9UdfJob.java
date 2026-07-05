package com.flywhl.flinklab.e05;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * e05-C9 · UDF:标量函数(脱敏)+ 表函数(标签炸裂)。
 *
 * <p>要点:① 2.x 的 UDF 走类型注解体系(@DataTypeHint/@FunctionHint),
 * 不再依赖运行时反推;② UDTF 在 SQL 里用 `CROSS JOIN LATERAL TABLE(f(x))`;
 * ③ UDF 必须无状态、确定性(有外呼需求→Async I/O(e11)或 Lookup 源,别塞 UDF)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e05-sql \
 *          -Dexec.mainClass=com.flywhl.flinklab.e05.C9UdfJob
 */
public final class C9UdfJob {
    private C9UdfJob() {
    }

    /** 标量:手机号脱敏 138****1234。 */
    public static class MaskPhone extends ScalarFunction {
        public String eval(String phone) {
            return phone == null || phone.length() < 7
                    ? phone
                    : phone.substring(0, 3) + "****" + phone.substring(phone.length() - 4);
        }
    }

    /** 表函数:逗号分隔标签 → 多行。 */
    @FunctionHint(output = @DataTypeHint("ROW<tag STRING>"))
    public static class SplitTags extends TableFunction<org.apache.flink.types.Row> {
        public void eval(String tags) {
            for (String s : tags.split(",")) {
                collect(org.apache.flink.types.Row.of(s));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.createTemporarySystemFunction("mask_phone", MaskPhone.class);
        t.createTemporarySystemFunction("split_tags", SplitTags.class);

        t.executeSql("""
                CREATE TABLE users (
                    uid INT,
                    phone AS CONCAT('138', LPAD(CAST(uid * 7919 AS STRING), 8, '0')),
                    tags  AS CASE MOD(uid, 3) WHEN 0 THEN 'vip,new'
                                              WHEN 1 THEN 'svip'
                                              ELSE 'new,coupon,ios' END
                ) WITH ('connector'='datagen','rows-per-second'='3',
                        'fields.uid.min'='1','fields.uid.max'='999')""");

        t.executeSql("""
                CREATE TABLE udf_out (uid INT, masked STRING, tag STRING)
                WITH ('connector'='print')""");

        t.executeSql("""
                INSERT INTO udf_out
                SELECT u.uid, mask_phone(u.phone), tg.tag
                FROM users u
                CROSS JOIN LATERAL TABLE(split_tags(u.tags)) AS tg(tag)""")
         .await();
    }
}
