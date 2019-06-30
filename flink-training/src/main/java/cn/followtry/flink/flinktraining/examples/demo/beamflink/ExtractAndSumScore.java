package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import cn.followtry.app.NameCount;
import cn.followtry.app.UserInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/29
 */
public class ExtractAndSumScore extends PTransform<PCollection<UserInfo>, PCollection<KV<String, NameCount>>> {

    /**  */
    private String field;

    public ExtractAndSumScore(String field) {
        this.field = field;
    }

    @Override
    public PCollection<KV<String, NameCount>> expand(PCollection<UserInfo> input) {
        //自定义 combine 的方法实现自由内容的转换，Aggr.ofCnt()
        /**
         * MyTypeDescriptors 用来实现可以存储自定义的 Bean
         * CntBeanFn 用来实现对特定逻辑的计算
         */

        return input
                .apply(
                        //通过该方式将 Bean 转为 KV 形式，方便对每个 key 进行分组统计
                        MapElements.into(
                                TypeDescriptors.kvs(TypeDescriptors.strings(), MyTypeDescriptors.userInfos()))
                                .via((UserInfo userInfo) -> KV.of(userInfo.getKey(field), userInfo)))
                .apply(ParDo.of(new DoFn<KV<String, UserInfo>, KV<String, NameCount>>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, UserInfo> element = c.element();
                UserInfo value = element.getValue();
                String key = element.getKey();
                NameCount nameCount = new NameCount();
                nameCount.setName(value.getName());
                nameCount.setCount(1);
                KV<String, NameCount> outputResult = KV.of(key, nameCount);
                System.out.println("new name count:"+nameCount);
                c.outputWithTimestamp(outputResult, new Instant(c.timestamp()));
            }
        }));

    }
}
