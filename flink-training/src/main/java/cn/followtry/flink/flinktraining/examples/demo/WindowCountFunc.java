package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.NameCount;
import cn.followtry.app.UserInfo;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class WindowCountFunc extends RichWindowFunction<UserInfo, NameCount, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<UserInfo> iterable, Collector<NameCount> collector) throws Exception {
        Integer count =0;
        for (UserInfo userInfo : iterable) {
            if (Objects.equals(key,userInfo.getName())){
                count++;
            }
        }
        List<UserInfo> detailItems = Lists.newArrayList(iterable);
        NameCount nameCount = new NameCount();
        nameCount.setName(key);
        nameCount.setCount(count);
        nameCount.setStartTime(DateFormatUtils.format(timeWindow.getStart(),"yyyy-MM-dd HH:mm:ss"));
        nameCount.setEndTime(DateFormatUtils.format(timeWindow.getEnd(),"yyyy-MM-dd HH:mm:ss"));
        nameCount.setDetailItems(detailItems);
        collector.collect(nameCount);
    }
}
