package cn.followtry.flink.flinktraining.examples.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/7/3
 */
public class WindowMechanismTest {

    /**
     * main.
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Object> sour = env.addSource(null);
        DataStream<Object> data = sour.map(new MapFunction<Object, Object>() {
            @Override
            public Object map(Object o) throws Exception {
                return null;
            }
        });


        data.keyBy(0).timeWindow(Time.seconds(10)).sum(1);
    }
}
