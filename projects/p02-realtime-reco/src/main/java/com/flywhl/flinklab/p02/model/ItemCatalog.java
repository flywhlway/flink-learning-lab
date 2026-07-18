package com.flywhl.flinklab.p02.model;

/**
 * 候选商品目录项（PG 维表 / 内存 catalog，D-05）。
 *
 * <p>字段：itemId / category / title / baseWeight。
 */
public final class ItemCatalog {

    public String itemId;
    public String category;
    public String title;
    public double baseWeight;

    public ItemCatalog() {
    }

    public ItemCatalog(String itemId, String category, String title, double baseWeight) {
        this.itemId = itemId;
        this.category = category;
        this.title = title;
        this.baseWeight = baseWeight;
    }

    public String itemId() {
        return itemId;
    }

    public String category() {
        return category;
    }

    public String title() {
        return title;
    }

    public double baseWeight() {
        return baseWeight;
    }
}
