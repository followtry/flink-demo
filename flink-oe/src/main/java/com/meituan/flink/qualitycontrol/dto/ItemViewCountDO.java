package com.meituan.flink.qualitycontrol.dto;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class ItemViewCountDO {

    /**  */
    public String key;

    /**  */
    public String windowStart;

    public String windowEnd;

    public Long windowEndTs;

    /**  */
    public Long count;

    public static ItemViewCountDO of(String key,String windowStart, String windowEnd,Long windowEndTs, Long count) {
        ItemViewCountDO itemViewCountDO = new ItemViewCountDO();
        itemViewCountDO.key = key;
        itemViewCountDO.windowStart = windowStart;
        itemViewCountDO.windowEnd = windowEnd;
        itemViewCountDO.windowEndTs = windowEndTs;
        itemViewCountDO.count = count;
        return itemViewCountDO;
    }


    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", key='" + key + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", windowEndTs=" + windowEndTs +
                ", windowStart='" + windowStart + '\'' +
                '}';
    }
}
