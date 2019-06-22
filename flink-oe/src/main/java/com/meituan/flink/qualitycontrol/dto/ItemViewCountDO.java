package com.meituan.flink.qualitycontrol.dto;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class ItemViewCountDO {

    /**  */
    private String key;

    /**  */
    private Long windowStart;

    private Long windowEnd;

    /**  */
    private Long count;

    public static ItemViewCountDO of(String key,Long windowStart, Long windowEnd, Long count) {
        ItemViewCountDO itemViewCountDO = new ItemViewCountDO();
        itemViewCountDO.key = key;
        itemViewCountDO.windowStart = windowStart;
        itemViewCountDO.windowEnd = windowEnd;
        itemViewCountDO.count = count;
        return itemViewCountDO;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", key='" + key + '\'' +
                ", windowEnd=" + windowEnd +
                ", windowStart=" + windowStart +
                '}';
    }
}
