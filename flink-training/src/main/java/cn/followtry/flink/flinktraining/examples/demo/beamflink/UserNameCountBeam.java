package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import cn.followtry.app.NameCount;
import cn.followtry.app.UserInfo;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 计算每段时间每个用户 id 的出现次数
 * @author jingzhongzhi
 * @since 2019-01-13
 */
public class UserNameCountBeam {

    private static final Logger log = LoggerFactory.getLogger(UserNameCountBeam.class);

    public static final String ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static final int topN = 10;
    /**
     *
     * 请求参数
     *  --brokers localhost:9092 --topic beam-on-flink
     * main.
     */
    public static void main(String[] args) throws Exception {

        /***===========--------解析参数--------==================*/
        ParameterTool tool = ParameterTool.fromArgs(args);
        String brokers = tool.get("brokers","localhost:9092");
        String topic = tool.get("topic","beam-on-flink");
        Properties properties = new Properties();
        String brokerServerList = brokers;
        String firstTopic = topic == null ? "beam-on-flink" : topic;
        String secondTopic = "beam-on-flink-res";
        properties.setProperty("bootstrap.servers", brokerServerList);
        properties.setProperty("group.id", "consumer-flink-beam");
        properties.setProperty("zookeeper.connect",ZK_HOSTS);

        /***===========--------执行环境--------==================*/
        // 创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定 PipelineRunner：FlinkRunner 必须指定如果不指定则为本地
        options.setRunner(FlinkRunner.class);
        options.setJobName("beam-running-on-flink");
        // 设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        /***===========--------设置数据源--------==================*/
        // 这里 kV 后说明 kafka 中的 key 为 Long 类型 和 value 为 String 类型
        PCollection<KV<Long, String>> lines = pipeline.apply("real_from_kafka", KafkaIO.<Long, String>read()
                // 必需设置 kafka 的服务器地址和端口
                .withBootstrapServers(brokerServerList)
                // 必需设置要读取的 kafka 的 topic 名称
                .withTopic(firstTopic)
                // 必需反序列化 key和 value
                .withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
                //"group.id", "consumer-flink-beam", "auto.offset.reset", "earliest"
                .updateConsumerProperties(ImmutableMap.of("group.id", "consumer-flink-beam"))
                .withReadCommitted().commitOffsetsInFinalize().withoutMetadata()
        );
        PCollection<String> kafkaData = lines.apply("rmove kafka metadata", Values.create());


        /***===========--------transform--------==================*/
        //类型转换，将 String 类型转换为 Bean
        PCollection<UserInfo> parseJsonData = kafkaData.apply("parse json",MapElements.via(new SimpleFunction<String, UserInfo>() {
            @Override
            public UserInfo apply(String value) {
                UserInfo userInfo = null;
                try {
                    userInfo = JSON.parseObject(value, UserInfo.class);
                    System.out.println("parse json suc. json is : " + value);
                } catch (Exception e) {
                    System.out.println("parse json error. json is : " + value);
                }
                return userInfo;
            }
        }));

        //过滤数据
        PCollection<UserInfo> filterData = parseJsonData.apply("filter", Filter.by(new ProcessFunction<UserInfo, Boolean>() {
            @Override
            public Boolean apply(UserInfo input) throws Exception {
                return input != null && input.getEventTime() != null && input.getName() != null;
            }
        }));


        // Add an element timestamp based on the event log
        PCollection<UserInfo> timestampData = filterData.apply("add timestamp", WithTimestamps.of(new SimpleFunction<UserInfo, Instant>() {
            @Override
            public Instant apply(UserInfo input) {
                Long eventTime = input.getEventTime() + 1000;
                Instant instant = new Instant(eventTime);
                System.out.println("event timestamp :" + instant.toDate());
                return instant;
            }
        }));

        //设置触发器和水位线
        AfterWatermark.AfterWatermarkEarlyAndLate watermarkEarlyAndLate = AfterWatermark.pastEndOfWindow()
                // During the month, get near real-time estimates.
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5)))
                // Fire on any late data so the bill can be corrected.
                .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)));

        //提供窗口
        PCollection<UserInfo> windowData = timestampData
                .apply("slide window",Window.<UserInfo>into(SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(3)))
                        .triggering(watermarkEarlyAndLate)
                        .withAllowedLateness(Duration.standardSeconds(3))
                        .accumulatingFiredPanes()
                        );


        PCollection<KV<String, NameCount>> countData = windowData.apply("count", new ExtractAndSumScore("name"));


        PCollection<String> result = countData.apply("concat key-value", MapElements.via(new SimpleFunction<KV<String, NameCount>, String>() {
            @Override
            public String apply(KV<String, NameCount> input) {
                System.out.print(" 进行统计：" + input.getKey() + ": " + JSON.toJSONString(input.getValue())+"\n");
                return input.getKey() + ": " + JSON.toJSONString(input.getValue());
            }
        }));


        /***===========--------sink to out--------==================*/
        //sink 到 kafka中
        sink2Kafka(brokerServerList, secondTopic, result);

        /***===========--------execute--------==================*/
        pipeline.run().waitUntilFinish();

    }

    private static void sink2Kafka(String brokerServerList, String secondTopic, PCollection<String> kvColl) {
        kvColl.apply(KafkaIO.<Void,String>write().
                withBootstrapServers(brokerServerList) // 设置写会 kafka 的集群配置地址
                .withTopic(secondTopic) // 设置返回 kafka 的消息主题
                .withValueSerializer(StringSerializer.class)
//                .updateProducerProperties(ImmutableMap.of("compression.type", "gzip"))
//                .withInputTimestamp()
                // Dataflow runner and Spark 兼容， Flink 对 kafka0.11 才支持
//                .withEOS(3, "eos-sink-group-id")
                // 只需要在此写入默认的 key 就行了，默认为 null 值
                .values()
        );
    }
}
