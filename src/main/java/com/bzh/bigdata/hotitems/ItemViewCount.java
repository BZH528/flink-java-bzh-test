package com.bzh.bigdata.hotitems;

public class ItemViewCount {

    public long itemId; // 商品id
    public long windowEnd;// 窗口结束时间戳
    public long viewCount;// 商品的点击量

    public ItemViewCount() {
    }

    public ItemViewCount(long itemId, long windowEnd, long viewCount) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
