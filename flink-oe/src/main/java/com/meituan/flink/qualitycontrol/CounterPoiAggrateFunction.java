package com.meituan.flink.qualitycontrol;

import com.meituan.flink.qualitycontrol.dto.GcResult;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class CounterPoiAggrateFunction implements AggregateFunction<GcResult, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(GcResult gcResult, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
