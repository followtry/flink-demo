package com.meituan.flink;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer010;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import com.meituan.flink.common.kafka.MTKafkaProducer08;
import com.meituan.flink.func.IdentityRead;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by liming on 2018/12/27.
 * FlinkCommon simple demo
 */
public class FlinkKafkaSimpleDemo {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========================== 初始化app config,从args中获取 ==========================//

        ParameterTool params = ParameterTool.fromArgs(args);
        long logIntervalMills = params.getLong("log_interval_mills");
        Configuration config = new Configuration();
        config.setLong("log_interval_mills", logIntervalMills);
        env.getConfig().setGlobalJobParameters(config);


        // ========================== 初始化topic consumer,产生source datastream ========================== //

        //获取所有非权限topic的MTKafkaConsumer08
        MTKafkaConsumer08 unAuthMtKafkaConsumer = new MTKafkaConsumer08(args);
        //获取所有权限topic的MTKafkaConsumer010
        MTKafkaConsumer010 authMtKafkaConsumer = new MTKafkaConsumer010(args);

        //获取非权限consumer的topic-->consumer08的map
        Map<KafkaTopic, FlinkKafkaConsumerBase> unAuthTopic2consumers = unAuthMtKafkaConsumer.build(new SimpleStringSchema())
                .getSubscribedTopicsToConsumers();
        //获取权限consumer的topic-->consumer08的map
        Map<KafkaTopic, FlinkKafkaConsumerBase> authTopic2consumers = authMtKafkaConsumer.build(new SimpleStringSchema())
                .getSubscribedTopicsToConsumers();

        //按topic名称获取consumer,注意getConsumerByName可能返回null.也可以遍历topic2consumer的map按topic名称产生dataStream
        //namespace请前往kafka查看. kafka主页: online:http://kafka.data.sankuai.com/  qa:http://kafka.test.data.sankuai.com/
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> unAuthConsumerEntry = unAuthMtKafkaConsumer
                .getConsumerByName("your.unauth.topic.name", "your.unauth.topic.namespace");
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> authConsumerEntry = authMtKafkaConsumer
                .getConsumerByName("your.auth.topic.name", "your.auth.topic.namespace");

        DataStream<String> unAuthStream01 = env.addSource(unAuthConsumerEntry.getValue())
                .setParallelism(unAuthConsumerEntry.getKey().getParallelism())
                .uid("src-your.unauth.topic.name")
                .name("src-your.unauth.topic.name");
        DataStream<String> authStream01 = env.addSource(authConsumerEntry.getValue())
                .setParallelism(authConsumerEntry.getKey().getParallelism())
                .uid("src-your.auth.topic.name")
                .name("src-your.auth.topic.name");


        // ========================== source datastream发往下游并进行处理 ========================== //

        DataStream<String> unionStream = unAuthStream01.union(authStream01)
                .flatMap(new IdentityRead()).name("readAndLog");


        // ========================== 初始化kafka producer,添加sink ========================== //

        //获取所有非权限topic的MTKafkaProducer08
        MTKafkaProducer08 unAuthMtKafkaProducer = new MTKafkaProducer08(args);
        //获取所有权限topic的MTKafkaProducer010
        MTKafkaProducer010 authMtKafkaProducer = new MTKafkaProducer010(args);

        Map<KafkaTopic, FlinkKafkaProducer08> unAuthTopic2producers = unAuthMtKafkaProducer.build(new SimpleStringSchema())
                .getTargetTopicsToProducers();
        Map<KafkaTopic, FlinkKafkaProducer010> authTopic2producers = authMtKafkaProducer.build(new SimpleStringSchema())
                .getTargetTopicsToProducers();

        //按topic名称获取producerEntry,注意getProducerByName可能返回null.也可以遍历topic2producer的map按topic名称产生dataStream
        //namespace请前往kafka查看. kafka主页: online:http://kafka.data.sankuai.com/  qa:http://kafka.test.data.sankuai.com/
        Map.Entry<KafkaTopic, FlinkKafkaProducer08> unAuthProducerEntry = unAuthMtKafkaProducer.getProducerByName("your.unauth.topic.name");
        Map.Entry<KafkaTopic, FlinkKafkaProducer010> authProducerEntry = authMtKafkaProducer.getProducerByName("your.auth.topic.name");

        unionStream.addSink(unAuthProducerEntry.getValue())
                .setParallelism(unAuthProducerEntry.getKey().getParallelism())
                .uid("sink-your.unauth.topic.name")
                .name("sink-your.unauth.topic.name");
        unionStream.addSink(authProducerEntry.getValue())
                .setParallelism(authProducerEntry.getKey().getParallelism())
                .uid("sink-your.auth.topic.name")
                .name("sink-your.auth.topic.name");

        // ========================== 注意：必须调用env.execute,否则无法生成JobGraph ========================== //
        env.execute((new JobConf(args)).getJobName());
    }
}
