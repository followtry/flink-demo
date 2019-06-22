package com.meituan.flink;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.func.IdentityRead;
import com.meituan.flink.utils.KafkaUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkKafkaDemo {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // init app conf
        ParameterTool params = ParameterTool.fromArgs(args);
        long logIntervalMills = params.getLong("log_interval_mills");
        Configuration config = new Configuration();
        config.setLong("log_interval_mills", logIntervalMills);
        env.getConfig().setGlobalJobParameters(config);

        // get union un-authorization kafka streams
        DataStream<String> allUnAuthStreams = KafkaUtils.unionStreams(
                KafkaUtils.getAllUnAuthStream(env, args));

        // get union authorization kafka streams
        DataStream<String> allAuthStreams = KafkaUtils.unionStreams(
                KafkaUtils.getAllAuthStreams(env, args));

        DataStream<String> unionStream = allUnAuthStreams.union(allAuthStreams)
                .flatMap(new IdentityRead()).name("readAndLog");

        // add all un-authorization kafka sink
        KafkaUtils.addUnAuthKafkaSinks(unionStream, KafkaUtils.getAllUnAuthProducers(args));

        // add all authorization kafka sink
        KafkaUtils.addAuthKafkaSinks(unionStream, KafkaUtils.getAllAuthProducers(args));

        env.execute((new JobConf(args)).getJobName());
    }
}
