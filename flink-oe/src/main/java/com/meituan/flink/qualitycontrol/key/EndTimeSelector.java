package com.meituan.flink.qualitycontrol.key;

import com.meituan.flink.qualitycontrol.dto.ItemViewCountDO;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/22
 */
public class EndTimeSelector implements KeySelector<ItemViewCountDO,String>{

    @Override
    public String getKey(ItemViewCountDO value) throws Exception {
        return String.valueOf(value.getWindowStart());
    }
}
