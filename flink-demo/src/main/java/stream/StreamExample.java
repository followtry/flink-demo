package stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 流式处理的示例程序。每5秒钟输出一次求和结果
 *
 * @author jingzhongzhi
 * @since 2020/2/9
 */
public class StreamExample {
    
    private static final String host = "localhost";
    
    private static final Integer port = 10000;
    
    private static final Integer port2 = 10001;
    
    
    /**
     * 接收 localhost:10000 的 socket 链接的消息。
     *
     * socket连接使用 nc -lk 10000 来启动
     * main.
     */
    public static void main(String[] args) throws Exception{
        
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    
        DataStream<Tuple2<String, Integer>> dataStream =
                env.socketTextStream(host, port)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        
        //将结果打印到标准输出，控制台
//        dataStream.print();
        
        //将结果打印到 socket接收端
        dataStream.writeToSocket(host, port2, new SerializationSchema<Tuple2<String, Integer>>() {
            @Override
            public byte[] serialize(Tuple2<String, Integer> element) {
                String s = element.toString()+"\n";
                return  s.getBytes();
            }
        });
        env.execute("word count example");
    }
    
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : value.trim().split(" ")) {
                if (StringUtils.isNotBlank(word)) {
                    //单词总数加一
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
