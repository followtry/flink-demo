package com.meituan.flink.qualitycontrol.window;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class WindowResultFunction extends RichWindowFunction<Long,ItemViewCountDO, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCountDO> out) throws Exception {
        Long count = input.iterator().next();
        long start = window.getStart();
        String startTime = DateFormatUtils.format(new Date(start), "yyyy-MM-dd HH:mm:ss");
        long end = window.getEnd();
        String endTime = DateFormatUtils.format(new Date(end), "yyyy-MM-dd HH:mm:ss");
        ItemViewCountDO viewCountDO = ItemViewCountDO.of(key, startTime, endTime,end, count);
        out.collect(viewCountDO);
    }
}
