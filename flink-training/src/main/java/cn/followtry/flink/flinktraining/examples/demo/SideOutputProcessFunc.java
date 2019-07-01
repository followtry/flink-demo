package cn.followtry.flink.flinktraining.examples.demo;

import cn.followtry.app.NameCount;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/7/1
 */
public class SideOutputProcessFunc extends ProcessFunction<NameCount, NameCount> {

    /**
     *  侧输出需要指定id 和 TypeInformation
     */
   private OutputTag<NameCount> outputTag;

    public SideOutputProcessFunc(OutputTag<NameCount> outputTag) {
        super();
        this.outputTag = outputTag;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<NameCount> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void processElement(NameCount nameCount, Context context, Collector<NameCount> collector) throws Exception {
        nameCount.setName("side-output-"+nameCount.getName());
        context.output(outputTag,nameCount);
    }
}
