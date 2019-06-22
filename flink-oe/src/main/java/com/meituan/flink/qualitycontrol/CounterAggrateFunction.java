package com.meituan.flink.qualitycontrol;

import com.meituan.flink.qualitycontrol.dto.QualityControlResultMq;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class CounterAggrateFunction implements AggregateFunction<QualityControlResultMq, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(QualityControlResultMq qualityControlResultMq, Long acc) {
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
