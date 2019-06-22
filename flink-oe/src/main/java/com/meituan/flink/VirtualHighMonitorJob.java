package com.meituan.flink;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.qualitycontrol.CounterWindow;
import com.meituan.flink.qualitycontrol.QcJsonDataParse;
import com.meituan.flink.qualitycontrol.QualityControlResultMq;
import com.meituan.flink.qualitycontrol.VirtualHighKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Map;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class VirtualHighMonitorJob {

    /**
     * main.
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MTKafkaConsumer08 consumer08 = new MTKafkaConsumer08(args);
        consumer08.build(new org.apache.flink.api.common.serialization.SimpleStringSchema());
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerBaseEntry = consumer08.getConsumerByName("app.mafka.hotel.oe.qualitycontrol.virtualhigh", "rz_kafka08-default");

        DataStream source = env.addSource(consumerBaseEntry.getValue()).uid("1. src_topic_name").name("1. src_topic_name");



        DataStream<QualityControlResultMq> jsonData = source.rebalance().map(new QcJsonDataParse()).uid("2. parse json data").name("2. parse json data");
        DataStream<QualityControlResultMq> filterData = jsonData.filter(o -> o != null && o.getClientIp() != null).uid("3. filter null data").name("3. filter null data");

        KeyedStream<QualityControlResultMq, String> keyedStream = filterData.keyBy(new VirtualHighKeySelector());
        WindowedStream<QualityControlResultMq, String, TimeWindow> window = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(30)));
        window.apply((new CounterWindow())).uid("4. sum data by client ip");

        env.execute((new JobConf(args)).getJobName());
    }
}
