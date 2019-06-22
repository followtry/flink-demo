package com.meituan.flink.qualitycontrol;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class CounterWindow implements WindowFunction<QualityControlResultMq, Tuple2<String, Long>, String, TimeWindow>{
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<QualityControlResultMq> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
        Long count =0L;
        for (QualityControlResultMq resultMq : iterable) {
            if (Objects.equals(key, resultMq.getClientIp())) {
                count++;
            }
        }
        //统计总数
        Tuple2<String, Long> result = Tuple2.of(key, count);
        collector.collect(result);
    }
}
