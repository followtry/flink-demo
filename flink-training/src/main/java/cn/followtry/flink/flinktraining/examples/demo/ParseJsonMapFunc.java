package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.UserInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class ParseJsonMapFunc extends RichMapFunction<String, UserInfo> {

    private transient Counter counter;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        counter = this.getRuntimeContext().getMetricGroup().counter("ParseJsonMapFunc");
    }

    @Override
    public UserInfo map(String value) throws Exception {
        this.counter.inc();
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
