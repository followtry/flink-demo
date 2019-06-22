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
    private String windowStart;

    private String windowEnd;

    private Long windowEndTs;

    /**  */
    private Long count;

    public static ItemViewCountDO of(String key,String windowStart, String windowEnd,Long windowEndTs, Long count) {
        ItemViewCountDO itemViewCountDO = new ItemViewCountDO();
        itemViewCountDO.key = key;
        itemViewCountDO.windowStart = windowStart;
        itemViewCountDO.windowEnd = windowEnd;
        itemViewCountDO.windowEndTs = windowEndTs;
        itemViewCountDO.count = count;
        return itemViewCountDO;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowEndTs() {
        return windowEndTs;
    }

    public void setWindowEndTs(Long windowEndTs) {
        this.windowEndTs = windowEndTs;
    }

    @Override
    public String toString() {
        return "{" +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", key='" + key + '\'' +
                "count=" + count +
                '}';
    }
}
