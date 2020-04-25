package stream;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author jingzhongzhi
 * @since 2020/2/11
 */
public class StreamFromKafka {
    
    public static final String topic = "order_info";
    
    public static final Properties props = new Properties();
    
    public static final String brokers = "127.0.0.1:9002";
    
    public static final String ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    
    static {
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", "consumer-flink");
        props.setProperty("zookeeper.connect",ZK_HOSTS);
    }
    
    
    /**
     * main.
     */
    public static void main(String[] args) {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        /***===========--------设置数据源--------==================*/
        DataStream<String> source = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props)).name("kafak source");
        env.enableCheckpointing(1000);
    
    
    
    
        /***===========--------transform--------==================*/
        DataStream<Order> result = source
                .map((MapFunction<String, Order>) value -> JSON.parseObject(value, Order.class)).name("string2order")
                .keyBy("orderId")
                .timeWindow(Time.seconds(10)).sum("orderId");
    
//        DataStream<String> dateStreamRes = dataStream.map(WC::toString);
//
//        /***===========--------sink to out--------==================*/
//        //sink 到 kafka中
//        sink2Kafka(brokerServerList, secondTopic, dateStreamRes);
//
//        /***===========--------execute--------==================*/
//        env.execute("Window WordCount");
        
    }
    
    public static class Order {
        private Long orderId;
        
        private String orderName;
        
        private Long ts;
    
        public Long getOrderId() {
            return orderId;
        }
    
        public void setOrderId(Long orderId) {
            this.orderId = orderId;
        }
    
        public String getOrderName() {
            return orderName;
        }
    
        public void setOrderName(String orderName) {
            this.orderName = orderName;
        }
    
        public Long getTs() {
            return ts;
        }
    
        public void setTs(Long ts) {
            this.ts = ts;
        }
    }
}
