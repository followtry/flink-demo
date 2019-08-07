package com.meituan.flink;

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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
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

        DataStream<GcResult> flatMapData = filterData.map(QualityControlResultMq::getResults).flatMap(new GetGcresult()).name("3.1 QualityControlResultMq -> GcResult");

        //使用 aggregate 的方式先预聚合计算，内存中存的聚合后的数据非明细数据
        DataStream<ItemViewCountDO> windowdData = flatMapData.keyBy(new PoiIdSelector())
                .window(SlidingProcessingTimeWindows.of(Time.minutes(30), Time.minutes(5)))
                .aggregate(new CounterPoiAggrateFunction(),new WindowResultFunction()).name("4. aggregate data by poi id");
        //参考文章： https://yq.aliyun.com/articles/706029
        DataStream<String> processData = windowdData.keyBy("windowEnd").process(new TopNHotItems(5)).name("5. process top N");
        processData.addSink(new SinkConsole3()).name("6. sink to console");
        env.execute((new JobConf(args)).getJobName());
    }

    public static class GetGcresult implements FlatMapFunction<List<GcResult>,GcResult> {
        @Override
        public void flatMap(List<GcResult> value, Collector<GcResult> out) throws Exception {
            for (GcResult gcResult : value) {
                out.collect(gcResult);
            }
        }
    }
}
