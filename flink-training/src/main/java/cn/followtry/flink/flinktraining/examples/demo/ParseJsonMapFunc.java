package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.UserInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class ParseJsonMapFunc extends RichMapFunction<String, UserInfo> {
    @Override
    public UserInfo map(String value) throws Exception {
        UserInfo userInfo = null;
        try {
            userInfo = JSON.parseObject(value, UserInfo.class);
            System.out.println("parse json suc. json is : " + value);
        } catch (Exception e) {
            System.out.println("parse json error. json is : " + value);
        }
        return userInfo;
    }
}
