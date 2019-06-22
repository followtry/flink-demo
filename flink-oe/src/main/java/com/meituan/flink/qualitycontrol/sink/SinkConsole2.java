package com.meituan.flink.qualitycontrol.sink;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import com.meituan.flink.utils.FbNxPublisherUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class SinkConsole2 implements SinkFunction<ItemViewCountDO> {
    private static final Logger log = LoggerFactory.getLogger(SinkConsole2.class);
    @Override
    public void invoke(ItemViewCountDO value, Context context) {
        System.out.println("=========计算结果: key="+value.getKey()+"windowEnd="+value.getWindowEnd()+",cnt="+value.getCount()+"===========");
        log.info("=========计算结果: key="+value.getKey()+"windowEnd="+value.getWindowEnd()+",cnt="+value.getCount()+"===========");
        String formatTime = DateFormatUtils.format(new Date(context.currentProcessingTime()), "yyyy-MM-dd HH:mm:ss");
        FbNxPublisherUtils.sendText("Flink 任务_时间2("+formatTime+")",value.toString(),"jingzhongzhi");
    }
}
