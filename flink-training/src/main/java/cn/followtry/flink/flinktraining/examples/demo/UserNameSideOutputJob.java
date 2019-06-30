package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.NameCount;
import cn.followtry.app.UserInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 计算每段时间每个用户 id 的出现次数
 * @author jingzhongzhi
 * @since 2019-01-13
 */
public class UserNameSideOutputJob {

    private static final Logger log = LoggerFactory.getLogger(UserNameSideOutputJob.class);

    public static final String ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static final int topN = 10;
    /**
     *
     * 请求参数
     *  --brokers localhost:9092 --topic beam-on-flink --showDetail false
     * main.
     */
    public static void main(String[] args) throws Exception {

        /***===========--------解析参数--------==================*/
        ParameterTool tool = ParameterTool.fromArgs(args);
        String brokers = tool.get("brokers","localhost:9092");
        String topic = tool.get("topic","beam-on-flink");
        boolean showDetail = tool.getBoolean("showDetail",false);
        Properties properties = new Properties();
        String brokerServerList = brokers;
        //"beam-on-flink";
        String firstTopic = topic;
        String secondTopic = "beam-on-flink-res";
        String thirdTopic = "beam-on-flink-side-output";

        properties.setProperty("bootstrap.servers", brokerServerList);
        properties.setProperty("group.id", "consumer-flink-side-output");
        properties.setProperty("zookeeper.connect",ZK_HOSTS);

        /***===========--------执行环境--------==================*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        /***===========--------设置数据源--------==================*/
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(firstTopic, new SimpleStringSchema(), properties);
        DataStream<String> source = env.addSource(flinkKafkaConsumer011).name("1. add source");


        /***===========--------transform--------==================*/
        DataStream<UserInfo> parseData =  source.rebalance().map(new ParseJsonMapFunc()).name("2. parse json");
        //只取 filter 结果为 true 的元素，
        DataStream<UserInfo> filterData =  parseData.filter(new FilterNullFunc()).name("3. filter data");
        DataStream<UserInfo> watermarkData =  filterData.assignTimestampsAndWatermarks(new MyWatermark()).name("watermark data");


        DataStream<NameCount> sumData = watermarkData
                .keyBy(new KeyNameSelector())
                .timeWindow(Time.minutes(1), Time.seconds(10))
                .apply(new WindowCountFunc(showDetail)).name("sum data");

        OutputTag<String> outputTag = new OutputTag<>("my-side-output");
        sumData.process(new ProcessFunction<NameCount, String>() {
            @Override
            public void processElement(NameCount nameCount, Context context, Collector<String> collector) throws Exception {
                context.output(outputTag,"side-output"+ JSON.toJSONString(nameCount));
            }
        });

        //第二个水印，聚合后的数据小于水印的就不要了
        DataStream<NameCount> secondWatermarkData =  sumData.assignTimestampsAndWatermarks(new MySecondWatermark()).name("second watermark data");

        //以窗口结束时间分组，排序不同的name 的 count
        DataStream<String> sortData = secondWatermarkData.keyBy("endTime").timeWindow(Time.seconds(10))
                .apply(new CountSortFunc(topN)).name("sort result");


//        DataStream<String> resultData = sumData.map(new Bean2StringMapFunc()).name("5. tuple2string");

        /***===========--------sink to out--------==================*/
        //sink 到 kafka中
        sink2Kafka(brokerServerList, secondTopic, sortData);

        /***===========--------execute--------==================*/
        env.execute("userName Count 2");

    }

    private static void sink2Kafka(String brokerServerList, String secondTopic, DataStream<String> dateStreamRes) {
        FlinkKafkaProducer011<String> sink2Kafka = new FlinkKafkaProducer011<>(brokerServerList,secondTopic, new SimpleStringSchema());
        dateStreamRes.addSink(sink2Kafka).name("sink 2 kafka");
    }
}
