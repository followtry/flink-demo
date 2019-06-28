package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.UserInfo;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class MyWatermark extends BoundedOutOfOrdernessTimestampExtractor<UserInfo> {

    public MyWatermark(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long getMaxOutOfOrdernessInMillis() {
        return super.getMaxOutOfOrdernessInMillis();
    }

    @Override
    public long extractTimestamp(UserInfo userInfo) {
        return userInfo.getEventTime();
    }
}
