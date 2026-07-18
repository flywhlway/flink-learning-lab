package com.flywhl.flinklab.p02.model;

/**
 * 候选商品目录项（Wave 0 {@code RuleScorerTest} 契约；与 {@link ItemCatalog} 字段对齐）。
 */
public final class CatalogItem {

    public String itemId;
    public String category;
    public String title;
    public double baseWeight;

    public CatalogItem() {
    }

    public CatalogItem(String itemId, String category, String title, double baseWeight) {
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

    public static CatalogItem from(ItemCatalog item) {
        if (item == null) {
            return null;
        }
        return new CatalogItem(item.itemId, item.category, item.title, item.baseWeight);
    }

    public ItemCatalog toItemCatalog() {
        return new ItemCatalog(itemId, category, title, baseWeight);
    }
}
