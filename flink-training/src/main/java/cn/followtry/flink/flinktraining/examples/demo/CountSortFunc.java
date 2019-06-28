package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.NameCount;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class CountSortFunc extends RichWindowFunction<NameCount,String, Tuple, TimeWindow> {

    private ListState<NameCount> listState;

    /** 排序前 N 的数据 */
    private Integer topN;

    public CountSortFunc(Integer topN) {
        this.topN = topN;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return super.getIterationRuntimeContext();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<NameCount> stateDescriptor = new ListStateDescriptor<>("name-count",NameCount.class);
        listState = this.getRuntimeContext().getListState(stateDescriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
        listState.clear();
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<NameCount> iterable, Collector<String> collector) throws Exception {
        List<NameCount> nameCounts = Lists.newArrayList(iterable);
        nameCounts.sort(new Comparator<NameCount>() {
            @Override
            public int compare(NameCount o1, NameCount o2) {
                return o2.getCount() - o1.getCount();
            }
        });

        if (nameCounts.size() < topN) {
            topN = nameCounts.size();
        }
        StringBuilder result = new StringBuilder("====================统计排序:====================\n");
        result.append("明细数据: ").append(JSON.toJSONString(nameCounts)).append(" \n");
        String startTime = DateFormatUtils.ISO_DATETIME_FORMAT.format(timeWindow.getStart());
        String endTime = DateFormatUtils.ISO_DATETIME_FORMAT.format(timeWindow.getEnd());
        result.append("窗口时间: [").append(startTime).append(",").append(endTime).append("]\n");
        for (int i = 0; i < topN; i++) {
            NameCount nameCount = nameCounts.get(i);
            result.append("i=").append((i+1))
                    .append(",name=").append(nameCount.getName())
                    .append(",count=").append(nameCount.getCount())
                    .append(",start=").append(nameCount.getStartTime())
                    .append(",end=").append(nameCount.getEndTime())
            ;
            result.append("\n");
        }
        result.append("===========================结束==============================\n");
        collector.collect(result.toString());
    }
}
