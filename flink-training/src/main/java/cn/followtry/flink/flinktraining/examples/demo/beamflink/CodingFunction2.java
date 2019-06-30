package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import java.io.Serializable;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/30
 */
public interface CodingFunction2<InputT, OutputT> extends Serializable {
    OutputT apply(InputT input) throws Exception;
}