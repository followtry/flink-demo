package cn.followtry.flink.flinktraining.examples.demo.func;

import cn.followtry.app.UserInfo;
import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class FilterNullFunc extends RichFilterFunction<UserInfo> {

    /**
     * 只取结果为 true 的元素，判断结果为 false 的元素会被丢弃
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public boolean filter(UserInfo value) throws Exception {
        return value != null && value.getName() != null && value.getEventTime() != null;
    }
}
