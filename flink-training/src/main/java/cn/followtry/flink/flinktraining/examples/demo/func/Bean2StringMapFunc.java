package cn.followtry.flink.flinktraining.examples.demo.func;

import cn.followtry.app.NameCount;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class Bean2StringMapFunc extends RichMapFunction<NameCount, String> {
    @Override
    public String map(NameCount value) throws Exception {
        return JSON.toJSONString(value);
    }
}
