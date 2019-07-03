package cn.followtry.flink.flinktraining.examples.demo.func;

import cn.followtry.app.UserInfo;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class KeyNameSelector implements KeySelector<UserInfo, String> {
    @Override
    public String getKey(UserInfo value) throws Exception {
        return value.getName();
    }
}
