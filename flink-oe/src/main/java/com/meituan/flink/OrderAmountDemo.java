package com.meituan.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.common.kafka.MTKafkaProducer08;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.Map;

/**
 * QuickStart: sum order amount group by dealId.
 * You can see the topic[log.orderlog] at http://kafka.data.sankuai.com/
 */
public class OrderAmountDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // ========================== 初始化app config,从args中获取,按需设置env config==========================//

        String jobName = new JobConf(args).getJobName();


        // ========================== 初始化topic consumer,产生source datastream ========================== //

        //声明source流，按需声明
        DataStream<String> logOrderLogStream = null;
        //获取所有非权限topic的MTKafkaConsumer08
        MTKafkaConsumer08 unAuthMtKafkaConsumer = new MTKafkaConsumer08(args);

        //TODO 获取组内权限topic的consumer
        //MTKafkaConsumer010 authMtKafkaConsumer = new MTKafkaConsumer010(args);

        //获取非权限consumer的topic-->consumer08的map
        Map<KafkaTopic, FlinkKafkaConsumerBase> unAuthTopic2consumers = unAuthMtKafkaConsumer.build(new SimpleStringSchema())
                .getSubscribedTopicsToConsumers();

        //遍历map 按照topic名称获取consumer并给source流赋值，也可调用getConsumerByName，详见FlinkKafkaSimpleDemo
        for(Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> entry:unAuthTopic2consumers.entrySet()){
            if(entry.getKey().getTopicName().equals("log.orderlog")){
                logOrderLogStream = env.addSource(entry.getValue()).name("source-log.orderlog");
            }else{
                //write your code
            }
        }


        // ========================== source datastream发往下游并进行处理 ========================== //

        // parse the data, group it, window it, and aggregate the counts
        DataStream<Tuple2<Integer, Double>> orderAmount =
                logOrderLogStream.map(new ParseOrderRecord())
                        .filter(new RecordFilter())
                        .keyBy("dealId")
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                        .aggregate(new AmountSum());

        DataStream<String> result = orderAmount.map(new FormatResult());


        // ========================== 初始化kafka producer,添加sink ========================== //

        //获取所有非权限topic的MTKafkaProducer08
        MTKafkaProducer08 unAuthMtKafkaProducer = new MTKafkaProducer08(args);

        //TODO 获取所有权限topic的MTKafkaProducer010
        //MTKafkaProducer010 authMtKafkaProducer = new MTKafkaProducer010(args);

        //获取非权限producer的topic-->producer08的map
        Map<KafkaTopic, FlinkKafkaProducer08> unAuthTopic2producers = unAuthMtKafkaProducer.build(new SimpleStringSchema())
                .getTargetTopicsToProducers();

        //遍历map 按照topic名称获取producer并给sink流赋值，也可调用getProducerByName，详见FlinkKafkaSimpleDemo
        for(Map.Entry<KafkaTopic, FlinkKafkaProducer08> entry:unAuthTopic2producers.entrySet()){
            if(entry.getKey().getTopicName().equals("log.lds_test")){
                result.addSink(entry.getValue()).name("sink-log.lds_test");
            }else{
                //write your code
            }
        }

        // ========================== 注意：必须调用env.execute,否则无法生成JobGraph ========================== //
        env.execute("OrderAmount!");
    }

    public static class ParseOrderRecord implements MapFunction<String, OrderRecord> {

        @Override
        public OrderRecord map(String s) throws Exception {
            JSONObject jsonObject = JSON.parseObject(s);
            long id = jsonObject.getLong("id");
            int dealId = jsonObject.getInteger("dealid");
            String action = jsonObject.getString("_mt_action");
            double amount = jsonObject.getDouble("amount");

            return new OrderRecord(id, dealId, action, amount);
        }
    }

    public static class RecordFilter implements FilterFunction<OrderRecord> {

        @Override
        public boolean filter(OrderRecord orderRecord) throws Exception {
            return "PAY".equals(orderRecord.action);
        }
    }

    public static class AmountSum implements AggregateFunction<
                    OrderRecord, // input type
                    Tuple2<Integer, Double>, //intermediate aggregate state
                    Tuple2<Integer, Double>> {  // output type


        @Override
        public Tuple2<Integer, Double> createAccumulator() {
            return new Tuple2<>(0, 0.0);
        }

        @Override
        public Tuple2<Integer, Double> add(OrderRecord r, Tuple2<Integer, Double> accAmount) {
            Double curAmount = accAmount.getField(1);
            accAmount.setField(r.dealId ,0);
            accAmount.setField(curAmount + r.amount, 1);
            return accAmount;
        }

        @Override
        public Tuple2<Integer, Double> getResult(Tuple2<Integer, Double> accAmount) {
            return accAmount;
        }

        @Override
        public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
            return a; //not merge
        }
    }

    public static class FormatResult implements MapFunction<Tuple2<Integer, Double>, String> {

        @Override
        public String map(Tuple2<Integer, Double> t) throws Exception {
            return t.getField(0) + ": " + t.getField(1);
        }
    }

    public static class OrderRecord {

        public long id;
        public int dealId;
        public String action;
        public double amount;

        public OrderRecord() {}

        public OrderRecord(long id, int dealId, String action, double amount) {
            this.id = id;
            this.dealId = dealId;
            this.action = action;
            this.amount = amount;
        }
    }
}
