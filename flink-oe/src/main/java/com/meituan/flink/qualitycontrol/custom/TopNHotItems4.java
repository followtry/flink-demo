package com.meituan.flink.qualitycontrol.custom;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class TopNHotItems4 extends ProcessAllWindowFunction<List<ItemViewCountDO>,String,  TimeWindow> {

    /**  */
    private Integer topSize;

    public TopNHotItems4(Integer topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void process(Context context, java.lang.Iterable<List<ItemViewCountDO>> elements, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCountDO> allItems = new ArrayList<>();
        for (List<ItemViewCountDO> list : elements) {
            for (ItemViewCountDO item : list) {
                allItems.add(item);
            }
        }

        // 按照点击量从大到小排序
        allItems.sort(new Comparator<ItemViewCountDO>() {
            @Override
            public int compare(ItemViewCountDO o1, ItemViewCountDO o2) {
                return (int) (o2.getCount() - o1.getCount());
            }
        });
        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        String now = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
        result.append("==========当前时间:").append(now).append("==========================\n");
        //避免产品数量不够导致 NPE 的异常
        if (allItems.size() < topSize) {
            topSize = allItems.size();
        }
        for (int i=0;i<topSize;i++) {
            ItemViewCountDO currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No ").append(i+1).append(" :")
                    .append("  商品ID=").append(currentItem.getKey())
                    .append("  浏览量=").append(currentItem.getCount())
                    .append("  winStart=").append(currentItem.getWindowStart())
                    .append("  winEnd=").append(currentItem.getWindowEnd())
                    .append("\n");
        }
        result.append("====================================\n\n");
        Thread.sleep(1000);
        out.collect(result.toString());
    }
}


