package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.UserInfo;
import cn.followtry.flink.flinktraining.examples.demo.func.ParseJsonMapFunc;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 将 Stream 转为 table，并对 table 数据进行查询和输出到csv 文件和控制台
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
    public static void main(String[] args) throws Exception {

        /***===========--------解析参数--------==================*/
        ParameterTool tool = ParameterTool.fromArgs(args);
        String brokers = tool.get("brokers","localhost:9092");
        String topic = tool.get("topic","beam-on-flink");
        Properties properties = new Properties();
        String brokerServerList = brokers;
        //"beam-on-flink";
        String firstTopic = topic;
        String thirdTopic = "beam-on-flink-table-res";
        properties.setProperty("bootstrap.servers", brokerServerList);
        properties.setProperty("group.id", "consumer-flink-3");
        properties.setProperty("zookeeper.connect",ZK_HOSTS);

        /***===========--------执行环境--------==================*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        /***===========--------设置数据源--------==================*/
        FlinkKafkaConsumer<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer<>(firstTopic, new SimpleStringSchema(), properties);
        DataStream<String> source = env.addSource(flinkKafkaConsumer011).name("1. add source");
        DataStream<UserInfo> parseData =  source.rebalance().map(new ParseJsonMapFunc()).name("2. parse json");

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);


        /***===========--------注册 source 表和 sink 表--------==================*/
        tEnv.registerDataStream(TABLE_NAME_1,parseData,"id,name,eventTime,eventTimeStr");


        CsvTableSink csvTableSink = new CsvTableSink("/Users/jingzhongzhi/tmp/flink_table_result", ",",1, FileSystem.WriteMode.OVERWRITE);
        // register the TableSink with a specific schema
        String[] fieldNames = {"id", "name", "eventTimeStr"};
        TypeInformation[] fieldTypes = {Types.LONG(), Types.STRING(), Types.STRING()};
        String csvSinkTable = "CsvSinkTable";
        tEnv.registerTableSink(csvSinkTable, fieldNames, fieldTypes, csvTableSink);

        /***===========--------查询数据并写入到 sink 表--------==================*/
        Table userInfoTable = tEnv.scan(TABLE_NAME_1).select("id,name,eventTimeStr");
        userInfoTable.insertInto(csvSinkTable);


        /***===========------- 将查询出的表数据转换为 Stream 后写入的 sink 流中--------==================*/
        //将表转换为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(userInfoTable, Row.class);

        DataStream<String> outResult = retractStream.map(new MapFunction<Tuple2<Boolean, Row>, String>() {
            @Override
            public String map(Tuple2<Boolean, Row> tuple2) throws Exception {
                Long id = (Long) tuple2.f1.getField(0);
                String name = (String)tuple2.f1.getField(1);
                String eventTimeStr = (String)tuple2.f1.getField(2);

                UserInfo userInfo = new UserInfo();
                userInfo.setId(id);
                userInfo.setName(name);
                userInfo.setEventTimeStr(eventTimeStr);
                return JSON.toJSONString(userInfo);
            }
        });

        sink2Kafka(brokerServerList,thirdTopic,outResult);

        /***===========-------- 执行器--------==================*/
        env.execute("query flink table["+TABLE_NAME_1+"]");


    }

    private static void sink2Kafka(String brokerServerList, String secondTopic, DataStream<String> dateStreamRes) {
        FlinkKafkaProducer<String> sink2Kafka = new FlinkKafkaProducer<>(brokerServerList,secondTopic, new SimpleStringSchema());
        dateStreamRes.addSink(sink2Kafka).name("sink 2 kafka["+secondTopic+"]");
    }
}
