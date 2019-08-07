package com.meituan.flink.func;

import com.meituan.flink.qualitycontrol.dto.GcResult;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class GcResultWatermark extends AscendingTimestampExtractor<GcResult> {

    public GcResultWatermark() {
        super();
    }

    @Override
    public AscendingTimestampExtractor<GcResult> withViolationHandler(MonotonyViolationHandler handler) {
        return super.withViolationHandler(handler);
    }

    @Override
    public long extractAscendingTimestamp(GcResult userInfo) {
        return userInfo.getTime();
    }
}
