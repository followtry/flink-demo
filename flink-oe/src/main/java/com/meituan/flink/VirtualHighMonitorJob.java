package com.meituan.flink;

import com.alibaba.fastjson.JSON;
import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.qualitycontrol.CounterPoiAggrateFunction;
import com.meituan.flink.qualitycontrol.custom.TopNHotItems;
import com.meituan.flink.qualitycontrol.dto.GcResult;
import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import com.meituan.flink.qualitycontrol.dto.QualityControlResultMq;
import com.meituan.flink.qualitycontrol.key.PoiIdSelector;
import com.meituan.flink.qualitycontrol.parse.QcJsonDataParse;
import com.meituan.flink.qualitycontrol.sink.SinkConsole3;
import com.meituan.flink.qualitycontrol.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import java.util.List;
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

        DataStream<GcResult> flatData = filterData.flatMap(new FlatMapFunction<QualityControlResultMq, GcResult>() {
            @Override
            public void flatMap(QualityControlResultMq qualityControlResultMq, Collector<GcResult> collector) throws Exception {
                List<GcResult> results = qualityControlResultMq.getResults();
                if (CollectionUtil.isNullOrEmpty(results)) {
                    return;
                }
                results.forEach(collector::collect);
            }
        });

        //增加水印
        DataStream<GcResult> timedData = flatData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<GcResult>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(GcResult element) {
                return System.currentTimeMillis();
            }
        }).name("3.1 watermarks assign");

        //使用 aggregate 的方式先预聚合计算，内存中存的聚合后的数据非明细数据
        DataStream<ItemViewCountDO> windowdData = timedData.keyBy(new PoiIdSelector())
                .window(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.seconds(30)))
                .aggregate(new CounterPoiAggrateFunction(),new WindowResultFunction()).name("4. aggregate data by client appkey");
        //参考文章： https://yq.aliyun.com/articles/706029
        DataStream<String> processData = windowdData.keyBy("windowEnd").process(new TopNHotItems(5)).name("5. process top N");
        processData.addSink(new SinkConsole3()).name("6. sink to console");
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
