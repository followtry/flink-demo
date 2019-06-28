package cn.followtry.app;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class MyKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(MyKafkaProducer.class);


    public static final String ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static final String brokerServerList = "localhost:9092";

    public static final String topic = "beam-on-flink";

    /**  */
    private static Properties properties;

    /**
     * main.
     */
    public static void main(String[] args) {
        init();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 200; i++) {
            long eventTime = System.currentTimeMillis();
            String timeSuffix = DateFormatUtils.format(eventTime, "HH:mm");
//            timeSuffix = "-";
            UserInfo userInfo = new UserInfo(eventTime, "jingzhongzhi-"+ timeSuffix+"-" + (i / 10), i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, JSON.toJSONString(userInfo));
            producer.send(record, (metadata, exception) -> {
                long offset = metadata.offset();
                System.out.println("cur offset is :" + offset);
            });

            waitTime(200);
        }

    }

    private static void waitTime(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

    private static void init(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties = props;
    }
}
