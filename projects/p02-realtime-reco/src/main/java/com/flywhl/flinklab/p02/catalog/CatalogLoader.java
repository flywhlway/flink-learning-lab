package com.flywhl.flinklab.p02.catalog;

import com.flywhl.flinklab.p02.model.CatalogItem;
import com.flywhl.flinklab.p02.model.ItemCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL {@code reco_items} 全表加载（D-05）：供算子 {@code open()} 调用一次。
 *
 * <p>百级 item 内存打分；禁止每条 Async PG。Lab 账号 flinklab/flinklab123（T-05-06）。
 */
public final class CatalogLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogLoader.class);

    private static final String DEFAULT_USER = "flinklab";
    private static final String DEFAULT_PASSWORD = "flinklab123";

    private CatalogLoader() {
    }

    /**
     * 加载 {@code reco_items} → {@code Map<itemId, ItemCatalog>}。
     */
    public static Map<String, ItemCatalog> load(String jdbcUrl) {
        Map<String, ItemCatalog> map = new HashMap<>();
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            LOG.warn("pg jdbcUrl 为空，返回空 catalog");
            return map;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, DEFAULT_USER, DEFAULT_PASSWORD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT item_id, category, title, base_weight FROM reco_items")) {
            while (rs.next()) {
                String itemId = rs.getString("item_id");
                if (itemId == null || itemId.isBlank()) {
                    continue;
                }
                ItemCatalog item = new ItemCatalog(
                        itemId,
                        rs.getString("category"),
                        rs.getString("title"),
                        rs.getDouble("base_weight"));
                map.put(itemId, item);
            }
            LOG.info("CatalogLoader 加载 reco_items 完成，size={}", map.size());
        } catch (Exception e) {
            LOG.warn("CatalogLoader 加载失败，返回已读子集/空表: {}", e.toString());
        }
        return map;
    }

    /** 转为 Wave 0 / RuleScorer 使用的 {@link CatalogItem} 列表。 */
    public static List<CatalogItem> toCatalogItems(Map<String, ItemCatalog> catalog) {
        if (catalog == null || catalog.isEmpty()) {
            return List.of();
        }
        List<CatalogItem> list = new ArrayList<>(catalog.size());
        for (ItemCatalog item : catalog.values()) {
            list.add(CatalogItem.from(item));
        }
        return list;
    }

    /** itemId → category 索引，供 SessionFeature 类目亲和。 */
    public static Map<String, String> categoryIndex(Map<String, ItemCatalog> catalog) {
        if (catalog == null || catalog.isEmpty()) {
            return Map.of();
        }
        Map<String, String> idx = new HashMap<>();
        for (ItemCatalog item : catalog.values()) {
            if (item.itemId != null && item.category != null) {
                idx.put(item.itemId, item.category);
            }
        }
        return Map.copyOf(idx);
    }
}
