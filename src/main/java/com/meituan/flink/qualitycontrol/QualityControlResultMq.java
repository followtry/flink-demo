package com.meituan.flink.qualitycontrol;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author jingzhongzhi
 * @since 2018/3/11
 */
@Data
public class QualityControlResultMq implements Serializable{

    /**
     * 质控类型，1.实时质控
     */
    private Integer gcType;

    private List<GcResult> results;

    private String clientAppKey;

    private String clientIp;

    private String clientRequestparam;

}
