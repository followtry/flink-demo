package com.meituan.flink.qualitycontrol;

import com.meituan.flink.VirtualHighMonitorJob;
import com.meituan.flink.utils.FbNxPublisherUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class SinkConsole implements SinkFunction<VirtualHighMonitorJob.WC> {
    private static final Logger log = LoggerFactory.getLogger(SinkConsole.class);
    @Override
    public void invoke(VirtualHighMonitorJob.WC value, Context context) {
        System.out.println("=========计算结果: key="+value.getClientIp()+",cnt="+value.getCnt()+"===========");
        log.info("=========计算结果: key="+value.getClientIp()+",cnt="+value.getCnt()+"===========");
        FbNxPublisherUtils.sendText("Flink 任务_时间(context.currentProcessingTime())",value.toJsonString(),"jingzhongzhi");
    }
}
