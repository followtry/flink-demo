package cn.followtry.flink.flinktraining.examples.demo.func;

import cn.followtry.app.UserInfo;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class MyWatermark extends AscendingTimestampExtractor<UserInfo> {

    public MyWatermark() {
        super();
    }

    @Override
    public AscendingTimestampExtractor<UserInfo> withViolationHandler(MonotonyViolationHandler handler) {
        return super.withViolationHandler(handler);
    }

    @Override
    public long extractAscendingTimestamp(UserInfo userInfo) {
        return userInfo.getEventTime();
    }
}
