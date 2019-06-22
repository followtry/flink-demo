package com.meituan.flink.qualitycontrol;

import com.meituan.flink.VirtualHighMonitorJob;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class CounterWindow implements WindowFunction<QualityControlResultMq,VirtualHighMonitorJob.WC, String, TimeWindow>{
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<QualityControlResultMq> iterable, Collector<VirtualHighMonitorJob.WC> collector) throws Exception {
        Integer count =0;
        for (QualityControlResultMq resultMq : iterable) {
            if (Objects.equals(key, resultMq.getClientIp())) {
                count++;
            }
        }
        //统计总数
        VirtualHighMonitorJob.WC wc = new VirtualHighMonitorJob.WC(key,count);
        System.out.println("计算结果: " + wc.toJsonString());
        collector.collect(wc);
    }
}
