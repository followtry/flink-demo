package utils;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public class KafkaUtils {
    
    

    public static DataStream<String> unionStreams(List<DataStream<String>> streamList) {
        DataStream<String> rslt = null;

        for (DataStream<String> stream : streamList) {
            rslt = rslt == null ? stream : rslt.union(stream);
        }

        return rslt;
    }
}
