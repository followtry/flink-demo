package com.meituan.flink.qualitycontrol;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 质控结果
 * @author jingzhongzhi
 * @since 2018/3/11
 */
@Data
public class GcResult implements Serializable{

    private Long poiId;

    private Long goodsId;

    private Integer checkinDate;

    private List<QualityControlResultEnum> resultList;

    private Double salePrice;

    private Double mtPrice;

    private Double compPrice;

    private Double compCashPayPrice;

    private Double poiAvgPrice;

    private Double historyPrice;

    private Integer isHighStar;

    //GRA类型
    private Integer type;

    //是否匹配上COM
    private Boolean matched;

    //最低COM价匹配上的COMid 1-CT 2-EL 3-QN
    private Integer comPriceSiteId;

    private Integer comPricePayCashSiteId;

    private String lastCrawlTime;

    private Double poiLimitPrice;

    private Double goodsLimitPrice;

    /** 虚高原因 */
    @JSONField(name = "vhReason")
    private String virtualHighReason;

    private Integer virtualHighType;
}
