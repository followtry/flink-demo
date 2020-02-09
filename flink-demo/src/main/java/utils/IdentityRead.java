package utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityRead extends RichFlatMapFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(IdentityRead.class);
    private long logIntervalMills;
    private long lastLogTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig.GlobalJobParameters globals = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globalConf = (Configuration) globals;

        this.lastLogTime = System.currentTimeMillis();
        this.logIntervalMills = globalConf.getLong("log_interval_mills", 30000);
        LOG.info("log interval is: " + this.logIntervalMills);
    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        if (System.currentTimeMillis() - lastLogTime > logIntervalMills) {
            LOG.info(s);
            this.lastLogTime = System.currentTimeMillis();
            collector.collect(s);
        }
    }
}