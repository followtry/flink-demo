package com.meituan.flink.qualitycontrol.window;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class WindowResultFunction implements WindowFunction<Long,ItemViewCountDO, String, TimeWindow>{
    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCountDO> out) throws Exception {
        Long count = input.iterator().next();
        long start = window.getStart();
        long end = window.getEnd();
        ItemViewCountDO viewCountDO = ItemViewCountDO.of(key, start, end, count);
        out.collect(viewCountDO);
    }
}
