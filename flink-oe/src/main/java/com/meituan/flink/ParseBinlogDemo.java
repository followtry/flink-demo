package com.meituan.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.meituan.data.binlog.BinlogColumn;
import com.meituan.data.binlog.BinlogEntry;
import com.meituan.data.binlog.BinlogEntryUtil;
import com.meituan.data.binlog.BinlogRow;
import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer08;
import com.meituan.flink.common.kafka.MTKafkaProducer08;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Map;

/**
 * Created by lds on 2017/5/10.
 * 暂时不支持同时消费权限和非权限的binlog，未来会发布新的依赖版本
 * 消费非权限版binlog使用1.1.0-java，权限版使用2.0.4
 */
public class ParseBinlogDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // ========================== 初始化app config,从args中获取,按需设置env config==========================//

        String jobName = new JobConf(args).getJobName();


        // ========================== 初始化topic consumer,产生source datastream ========================== //

        //声明source流，按需声明
        DataStream<BinlogEntry> logOrderLogStream = null;
        //获取所有非权限topic的MTKafkaConsumer08
        MTKafkaConsumer08 unAuthMtKafkaConsumer = new MTKafkaConsumer08(args);

        //获取非权限consumer的topic-->consumer08的map
        Map<KafkaTopic, FlinkKafkaConsumerBase> unAuthTopic2consumers = unAuthMtKafkaConsumer.build(new RawSchema())
                .getSubscribedTopicsToConsumers();

        //TODO 获取组内权限topic的consumer
        //MTKafkaConsumer010 authMtKafkaConsumer = new MTKafkaConsumer010(args);
        //Map<KafkaTopic, FlinkKafkaConsumerBase> authTopic2consumers = authMtKafkaConsumer.build(new RawSchema())
                //.getSubscribedTopicsToConsumers();

        //遍历map 按照topic名称获取consumer并给source流赋值，也可调用getConsumerByName，详见FlinkKafkaSimpleDemo
        for (Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> entry : unAuthTopic2consumers.entrySet()) {
            if (entry.getKey().getTopicName().equals("binlog.order_binlog")) {
                logOrderLogStream = env.addSource(entry.getValue()).name("src-log.orderlog");
            } else {
                //write your code
            }
        }


        // ========================== source datastream发往下游并进行处理 ========================== //

        // parse the data
        DataStream<String> orderStream = logOrderLogStream.flatMap(new BinlogParse());


        // ========================== 初始化kafka producer,添加sink ========================== //

        //获取所有非权限topic的MTKafkaProducer08
        MTKafkaProducer08 unAuthMtKafkaProducer = new MTKafkaProducer08(args);

        //获取非权限producer的topic-->producer08的map
        Map<KafkaTopic, FlinkKafkaProducer08> unAuthTopic2producers = unAuthMtKafkaProducer.build(new SimpleStringSchema())
                .getTargetTopicsToProducers();

        //TODO 获取所有权限topic的MTKafkaProducer010
        //MTKafkaProducer010 authMtKafkaProducer = new MTKafkaProducer010(args);
        //Map<KafkaTopic, FlinkKafkaProducer010> authTopic2producers = authMtKafkaProducer.build(new SimpleStringSchema())
                //.getTargetTopicsToProducers();

        //遍历map 按照topic名称获取producer并给sink流赋值，也可调用getProducerByName，详见FlinkKafkaSimpleDemo
        for (Map.Entry<KafkaTopic, FlinkKafkaProducer08> entry : unAuthTopic2producers.entrySet()) {
            if (entry.getKey().getTopicName().equals("log.lds_test")) {
                orderStream.addSink(entry.getValue()).name("sink-log.lds_test");
            } else {
                //write your code
            }
        }

        // ========================== 注意：必须调用env.execute,否则无法生成JobGraph ========================== //
        env.execute("TestBinlog!");
    }


    private static class BinlogParse implements FlatMapFunction<BinlogEntry, String> {

        @Override
        public void flatMap(BinlogEntry value, Collector<String> out) throws Exception {
            long executeTime = value.getExecuteTime();
            String tableName = value.getTableName();
            String eventType = value.getEventType();

            if (tableName.matches("order_dealid_.*")) {
                for (BinlogRow row : value.getRowDatas()) {
                    OrderRecord orderRecord = new OrderRecord(executeTime, tableName, eventType);
                    Map<String, BinlogColumn> cur = row.getCurColumns();
                    Map<String, BinlogColumn> before = null;
                    if (eventType.equals(BinlogRow.EVENT_TYPE_UPDATE)) {
                        before = row.getBeforeColumns();
                    }

                    OrderFields curColumns = new OrderFields();
                    curColumns.setId(cur.get("id").getValue());
                    curColumns.setUserid(cur.get("userid").getValue());
                    curColumns.setOrdertime(cur.get("ordertime").getValue());
                    curColumns.setPaytime(cur.get("paytime").getValue());
                    curColumns.setAmount(cur.get("amount").getValue());
                    curColumns.setStatus(cur.get("status").getValue());
                    orderRecord.setCurColumns(curColumns);

                    if (before != null) {
                        OrderFields beforeColumns = new OrderFields();
                        beforeColumns.setPaytime(before.get("paytime").getValue());
                        beforeColumns.setStatus(before.get("status").getValue());
                        orderRecord.setBeforeColumns(beforeColumns);
                    } else {
                        orderRecord.setBeforeColumns(new OrderFields());
                    }

                    out.collect(JSON.toJSONString(orderRecord));
                }
            }
        }
    }

    public static class OrderRecord {

        public long executeTime;
        public String tableName;
        public String eventType;
        public OrderFields beforeColumns;
        public OrderFields curColumns;

        public OrderRecord(long executeTime, String tableName, String eventType) {
            this.executeTime = executeTime;
            this.tableName = tableName;
            this.eventType = eventType;
        }

        public long getExecuteTime() {
            return executeTime;
        }

        public void setExecuteTime(long executeTime) {
            this.executeTime = executeTime;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public OrderFields getBeforeColumns() {
            return beforeColumns;
        }

        public void setBeforeColumns(OrderFields beforeColumns) {
            this.beforeColumns = beforeColumns;
        }

        public OrderFields getCurColumns() {
            return curColumns;
        }

        public void setCurColumns(OrderFields curColumns) {
            this.curColumns = curColumns;
        }
    }

    public static class OrderFields {

        public String id;
        public String userid;
        public String ordertime;
        public String paytime;
        public String amount;
        public String status;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUserid() {
            return userid;
        }

        public void setUserid(String userid) {
            this.userid = userid;
        }

        public String getOrdertime() {
            return ordertime;
        }

        public void setOrdertime(String ordertime) {
            this.ordertime = ordertime;
        }

        public String getPaytime() {
            return paytime;
        }

        public void setPaytime(String paytime) {
            this.paytime = paytime;
        }

        public String getAmount() {
            return amount;
        }

        public void setAmount(String amount) {
            this.amount = amount;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    private static class RawSchema extends AbstractDeserializationSchema<BinlogEntry> {

        @Override
        public BinlogEntry deserialize(byte[] message) throws IOException {
            Entry entry = BinlogEntryUtil.deserializeFromProtoBuf(message);
            BinlogEntry binlogEntry = BinlogEntryUtil.serializeToBean(entry);

            return binlogEntry;
        }
    }

}
