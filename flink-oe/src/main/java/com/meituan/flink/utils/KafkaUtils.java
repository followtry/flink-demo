package com.meituan.flink.utils;

import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer010;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import com.meituan.flink.common.kafka.MTKafkaProducer08;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KafkaUtils {

    // consumer
    public static List<DataStream<String>> getAllUnAuthStream(StreamExecutionEnvironment env, String[] args) {
        List<DataStream<String>> streamList = new LinkedList<>();
        MTKafkaConsumer08 mtKafkaConsumer08 = new MTKafkaConsumer08(args);
        Map<KafkaTopic, FlinkKafkaConsumerBase> topic2consumers = mtKafkaConsumer08.build(new SimpleStringSchema())
                .getSubscribedTopicsToConsumers();
        generateStreams(topic2consumers, env, streamList);

        return streamList;
    }

    public static List<DataStream<String>> getAllAuthStreams(StreamExecutionEnvironment env, String[] args) {
        List<DataStream<String>> streamList = new LinkedList<>();
        MTKafkaConsumer010 mtKafkaConsumer010 = new MTKafkaConsumer010(args);

        Map<KafkaTopic, FlinkKafkaConsumerBase> topic2consumers = mtKafkaConsumer010.build(new SimpleStringSchema())
                .getSubscribedTopicsToConsumers();
        generateStreams(topic2consumers, env, streamList);

        return streamList;
    }

    public static DataStream<String> unionStreams(List<DataStream<String>> streamList) {
        DataStream<String> rslt = null;

        for (DataStream<String> stream : streamList) {
            rslt = rslt == null ? stream : rslt.union(stream);
        }

        return rslt;
    }

    private static void generateStreams(Map<KafkaTopic, FlinkKafkaConsumerBase> topic2consumers,
                                        StreamExecutionEnvironment env,
                                        List<DataStream<String>> streamList) {
        for (Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> entry : topic2consumers.entrySet()) {
            KafkaTopic tTopic = entry.getKey();
            FlinkKafkaConsumerBase consumer = entry.getValue();

            DataStream<String> dataStream = env.addSource(consumer)
                    .setParallelism(tTopic.getParallelism())
                    .uid("src-" + tTopic.getTopicName())
                    .name("src-" + tTopic.getTopicName());
            streamList.add(dataStream);
        }
    }

    // producer
    public static Map<KafkaTopic, FlinkKafkaProducer08> getAllUnAuthProducers(String[] args) {
        MTKafkaProducer08 mtKafkaProducer08 = new MTKafkaProducer08(args);

        return mtKafkaProducer08.build(new SimpleStringSchema()).getTargetTopicsToProducers();
    }

    public static Map<KafkaTopic, FlinkKafkaProducer010> getAllAuthProducers(String[] args) {
        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);

        return mtKafkaProducer010.build(new SimpleStringSchema()).getTargetTopicsToProducers();
    }

    public static void addUnAuthKafkaSinks(DataStream<String> stream,
                                           Map<KafkaTopic, FlinkKafkaProducer08> topic2producers) {
        for (Map.Entry<KafkaTopic, FlinkKafkaProducer08> entry : topic2producers.entrySet()) {
            KafkaTopic tmpTopic = entry.getKey();
            FlinkKafkaProducer08 tmpProducer = entry.getValue();

            stream.addSink(tmpProducer)
                    .setParallelism(tmpTopic.getParallelism())
                    .uid("dst-" + tmpTopic.getTopicName())
                    .name("dst-" + tmpTopic.getTopicName());
        }
    }

    public static void addAuthKafkaSinks(DataStream<String> stream,
                                         Map<KafkaTopic, FlinkKafkaProducer010> topic2producers) {
        for (Map.Entry<KafkaTopic, FlinkKafkaProducer010> entry : topic2producers.entrySet()) {
            KafkaTopic tmpTopic = entry.getKey();
            FlinkKafkaProducer010 tmpProducer = entry.getValue();

            stream.addSink(tmpProducer)
                    .setParallelism(tmpTopic.getParallelism())
                    .uid("dst-" + tmpTopic.getTopicName())
                    .name("dst-" + tmpTopic.getTopicName());
        }
    }
}
