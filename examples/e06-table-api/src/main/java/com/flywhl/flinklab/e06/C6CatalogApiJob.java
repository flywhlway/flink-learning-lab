package com.flywhl.flinklab.e06;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.Arrays;

/**
 * e06-C6 · Catalog 编程接口:平台如何"管理"表而不只是"使用"表。
 *
 * <p>Catalog 是三级命名空间(catalog.database.table)的实现体。本例注册一个
 * 内存 Catalog、建库建表、再用编程接口枚举元数据 —— 这正是自研平台
 * "元数据服务/血缘采集"的原型;生产替身是 Hive/Paimon/JDBC Catalog(e09)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e06-table-api \
 *          -Dexec.mainClass=com.flywhl.flinklab.e06.C6CatalogApiJob
 */
public final class C6CatalogApiJob {
    private C6CatalogApiJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Catalog lab = new GenericInMemoryCatalog("lab_catalog", "ods");
        t.registerCatalog("lab_catalog", lab);
        t.useCatalog("lab_catalog");
        t.executeSql("CREATE DATABASE dwd");
        t.useDatabase("dwd");

        t.executeSql("""
                CREATE TABLE clicks (user_id INT, page STRING)
                WITH ('connector'='datagen','rows-per-second'='2',
                      'fields.user_id.min'='1','fields.user_id.max'='5',
                      'fields.page.length'='1')""");

        System.out.println("catalogs  = " + Arrays.toString(t.listCatalogs()));
        System.out.println("databases = " + Arrays.toString(t.listDatabases()));
        System.out.println("tables    = " + Arrays.toString(t.listTables()));
        System.out.println("full name = lab_catalog.dwd.clicks(三级命名空间)");

        t.executeSql("CREATE TABLE p (user_id INT, page STRING) WITH ('connector'='print')");
        t.executeSql("INSERT INTO p SELECT * FROM clicks").await();
    }
}
