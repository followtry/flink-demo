package cn.followtry.flink.flinktraining.examples.demo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class Tuple2StringMapFunc extends RichMapFunction<Tuple2<String,Integer>, String> {
    @Override
    public String map(Tuple2<String, Integer> value) throws Exception {
        return JSON.toJSONString(value);
    }
}
