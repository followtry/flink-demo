package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.NameCount;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class MySecondWatermark extends AscendingTimestampExtractor<NameCount> {

    public MySecondWatermark() {
        super();
    }

    @Override
    public AscendingTimestampExtractor<NameCount> withViolationHandler(MonotonyViolationHandler handler) {
        return super.withViolationHandler(handler);
    }

    @Override
    public long extractAscendingTimestamp(NameCount nameCount) {
        return nameCount.getEndTs();
    }
}
