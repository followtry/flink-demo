package cn.followtry.flink.flinktraining.examples.demo.beamflink;

/**
 * 聚合方法的工具类
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/30
 */
public class Aggr {

    public static CntBeanFn ofCnt() {
        return new CntBeanFn();
    }
}
