package com.meituan.flink.qualitycontrol.key;

import com.meituan.flink.qualitycontrol.dto.GcResult;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class PoiIdSelector implements KeySelector<GcResult,String>{

    @Override
    public String getKey(GcResult value) throws Exception {
        return String.valueOf(value.getPoiId());
    }
}
