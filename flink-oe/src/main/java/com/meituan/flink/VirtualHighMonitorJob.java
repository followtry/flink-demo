package com.meituan.flink;

import com.alibaba.fastjson.JSON;
import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.qualitycontrol.CounterAggrateFunction;
import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import com.meituan.flink.qualitycontrol.dto.QualityControlResultMq;
import com.meituan.flink.qualitycontrol.key.VirtualHighKeySelector;
import com.meituan.flink.qualitycontrol.parse.QcJsonDataParse;
import com.meituan.flink.qualitycontrol.sink.SinkConsole2;
import com.meituan.flink.qualitycontrol.window.WindowResultFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

        DataStream source = env.addSource(consumerBaseEntry.getValue()).name("1. src_topic_name");



        DataStream<QualityControlResultMq> jsonData = source.rebalance().map(new QcJsonDataParse()).name("2. parse json data");
        DataStream<QualityControlResultMq> filterData = jsonData.filter(o -> o != null && o.getClientAppKey() != null).uid("3. filter null data").name("3. filter null data");

        //使用 apply 的方式计算总数，是在窗口最后才计算，存储的是明细数据
        DataStream<ItemViewCountDO> source2 = filterData.keyBy(new VirtualHighKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new CounterAggrateFunction(),new WindowResultFunction()).name("4. sum data by client appkey");
        source2.addSink(new SinkConsole2()).name("5. sink to console");
        env.execute((new JobConf(args)).getJobName());
    }

    /**
     * 将Tuple 替换为 Pojo对象
     */
    public static class WC extends Tuple2<String,Integer> {

        /**  */
        private String clientAppkey;

        private Integer cnt;

        public WC() {
            super();
        }

        public WC(String word, Integer count) {
            super(word, count);
            this.clientAppkey = word;
            this.cnt = count;
        }

        public String getClientAppkey() {
            return getField(0);
        }

        public Integer getCnt() {
            return getField(1);
        }

        public void setClientAppkey(String clientAppkey) {
            this.clientAppkey = clientAppkey;
            setField(clientAppkey,0);
        }

        public String toJsonString() {
            return JSON.toJSONString(this);
        }


        @Override
        public String toString() {
            return "WC{" +
                    "clientAppkey='" + getClientAppkey() + '\'' +
                    ", cnt=" + getCnt() +
                    '}';
        }
    }
}
