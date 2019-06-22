package com.meituan.flink.qualitycontrol.key;

import com.meituan.flink.qualitycontrol.dto.QualityControlResultMq;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class VirtualHighKeySelector implements KeySelector<QualityControlResultMq,String>{

    @Override
    public String getKey(QualityControlResultMq value) throws Exception {
        return value.getClientAppKey();
    }
}
