package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.UserInfo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/7/2
 */
public class TableWordCountJob {

    private static final Logger log = LoggerFactory.getLogger(TableWordCountJob.class);

    public static final String ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static final int topN = 10;

    public static final String TABLE_NAME_1 = "user_info";

    /**
     * main.
     */
    public static void main(String[] args) {

        /***===========--------解析参数--------==================*/
        ParameterTool tool = ParameterTool.fromArgs(args);
        String brokers = tool.get("brokers","localhost:9092");
        String topic = tool.get("topic","beam-on-flink");
        boolean showDetail = tool.getBoolean("showDetail",false);
        Properties properties = new Properties();
        String brokerServerList = brokers;
        //"beam-on-flink";
        String firstTopic = topic;
        properties.setProperty("bootstrap.servers", brokerServerList);
        properties.setProperty("group.id", "consumer-flink-3");
        properties.setProperty("zookeeper.connect",ZK_HOSTS);

        /***===========--------执行环境--------==================*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        /***===========--------设置数据源--------==================*/
        FlinkKafkaConsumer<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer<>(firstTopic, new SimpleStringSchema(), properties);
        DataStream<String> source = env.addSource(flinkKafkaConsumer011).name("1. add source");
        DataStream<UserInfo> parseData =  source.rebalance().map(new ParseJsonMapFunc()).name("2. parse json");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        tEnv.registerDataStream(TABLE_NAME_1,parseData,"id,name,eventTime,eventTimeStr");



        CsvTableSink csvTableSink = new CsvTableSink("/Users/jingzhongzhi/tmp/flink_table_result", ",");
        // register the TableSink with a specific schema
        String[] fieldNames = {"id", "name", "eventTimeStr"};
        TypeInformation[] fieldTypes = {Types.LONG(), Types.STRING(), Types.STRING()};
        String csvSinkTable = "CsvSinkTable";
        tEnv.registerTableSink(csvSinkTable, fieldNames, fieldTypes, csvTableSink);

        Table userInfoTable = tEnv.scan(TABLE_NAME_1).select("id,name,eventTimeStr");

        userInfoTable.insertInto(csvSinkTable);

        tEnv.execEnv();


        //将表转换为流
//        DataStream<Tuple2<Boolean, UserInfo>> dataStream = tEnv.toRetractStream(userInfoTable, UserInfo.class);


    }
}
