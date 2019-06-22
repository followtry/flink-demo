package com.meituan.flink.qualitycontrol.custom;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
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
public class TopNHotItems extends KeyedProcessFunction<String,ItemViewCountDO,String> {
    
    /**  */
    private Integer topSize;

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<ItemViewCountDO> itemState;

    public TopNHotItems(Integer topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<ItemViewCountDO> itemsStateDesc = new ListStateDescriptor<>("itemState-state", ItemViewCountDO.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCountDO> allItems = new ArrayList<>();
        for (ItemViewCountDO item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
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
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        //避免产品数量不够导致 NPE 的异常
        if (allItems.size() < topSize) {
            topSize = allItems.size();
        }
        for (int i=0;i<topSize;i++) {
            ItemViewCountDO currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
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

    @Override
    public void processElement(ItemViewCountDO value, Context ctx, Collector<String> out) throws Exception {
        // 每条数据都保存到状态中
        itemState.add(value);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        ctx.timerService().registerProcessingTimeTimer(value.getWindowEndTs() + 1);
    }
}
