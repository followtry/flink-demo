package com.meituan.flink.qualitycontrol.sink;

import com.alibaba.fastjson.JSON;
import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import com.meituan.flink.utils.FbNxPublisherUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * 滑动窗口计算
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class SinkConsole4 implements SinkFunction<List<ItemViewCountDO>> {
    private static final Logger log = LoggerFactory.getLogger(SinkConsole4.class);
    @Override
    public void invoke(List<ItemViewCountDO> value, Context context) {
        String jsonString = JSON.toJSONString(value);
        System.out.println("=========计算结果: ===========\n"+ jsonString);
        log.info("=========计算结果: ===========\n"+ jsonString);
        String formatTime = DateFormatUtils.format(new Date(context.currentProcessingTime()), "yyyy-MM-dd HH:mm:ss");
        FbNxPublisherUtils.sendText("Flink 任务_时间2("+formatTime+")", jsonString,"jingzhongzhi");
    }
}
