package com.meituan.flink.qualitycontrol.sink;

import com.meituan.flink.utils.FbNxPublisherUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 滑动窗口计算
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class SinkConsole3 implements SinkFunction<String> {
    private static final Logger log = LoggerFactory.getLogger(SinkConsole3.class);
    @Override
    public void invoke(String value, Context context) {
        System.out.println("=========计算结果: content="+ value +"===========");
        log.info("=========计算结果: content="+ value +"===========");
        String formatTime = DateFormatUtils.format(new Date(context.currentProcessingTime()), "yyyy-MM-dd HH:mm:ss");
        FbNxPublisherUtils.sendText("Flink 任务_时间2("+formatTime+")",value,"jingzhongzhi");
    }
}
