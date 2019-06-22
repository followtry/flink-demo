package com.meituan.flink.qualitycontrol;

import com.meituan.flink.VirtualHighMonitorJob;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class SinkConsole implements SinkFunction<VirtualHighMonitorJob.WC> {
    @Override
    public void invoke(VirtualHighMonitorJob.WC value, Context context) {
        System.out.println("=========计算结果: key="+value.getClientIp()+",cnt="+value.getCnt()+"===========");
    }
}
