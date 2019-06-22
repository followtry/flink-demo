package com.meituan.flink.qualitycontrol.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

/**
 * 质控结果
 * @author jingzhongzhi
 * @since 2018/3/11
 */
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

    public Long getPoiId() {
        return poiId;
    }

    public void setPoiId(Long poiId) {
        this.poiId = poiId;
    }

    public Long getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Long goodsId) {
        this.goodsId = goodsId;
    }

    public Integer getCheckinDate() {
        return checkinDate;
    }

    public void setCheckinDate(Integer checkinDate) {
        this.checkinDate = checkinDate;
    }

    public List<QualityControlResultEnum> getResultList() {
        return resultList;
    }

    public void setResultList(List<QualityControlResultEnum> resultList) {
        this.resultList = resultList;
    }

    public Double getSalePrice() {
        return salePrice;
    }

    public void setSalePrice(Double salePrice) {
        this.salePrice = salePrice;
    }

    public Double getMtPrice() {
        return mtPrice;
    }

    public void setMtPrice(Double mtPrice) {
        this.mtPrice = mtPrice;
    }

    public Double getCompPrice() {
        return compPrice;
    }

    public void setCompPrice(Double compPrice) {
        this.compPrice = compPrice;
    }

    public Double getCompCashPayPrice() {
        return compCashPayPrice;
    }

    public void setCompCashPayPrice(Double compCashPayPrice) {
        this.compCashPayPrice = compCashPayPrice;
    }

    public Double getPoiAvgPrice() {
        return poiAvgPrice;
    }

    public void setPoiAvgPrice(Double poiAvgPrice) {
        this.poiAvgPrice = poiAvgPrice;
    }

    public Double getHistoryPrice() {
        return historyPrice;
    }

    public void setHistoryPrice(Double historyPrice) {
        this.historyPrice = historyPrice;
    }

    public Integer getIsHighStar() {
        return isHighStar;
    }

    public void setIsHighStar(Integer isHighStar) {
        this.isHighStar = isHighStar;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Boolean getMatched() {
        return matched;
    }

    public void setMatched(Boolean matched) {
        this.matched = matched;
    }

    public Integer getComPriceSiteId() {
        return comPriceSiteId;
    }

    public void setComPriceSiteId(Integer comPriceSiteId) {
        this.comPriceSiteId = comPriceSiteId;
    }

    public Integer getComPricePayCashSiteId() {
        return comPricePayCashSiteId;
    }

    public void setComPricePayCashSiteId(Integer comPricePayCashSiteId) {
        this.comPricePayCashSiteId = comPricePayCashSiteId;
    }

    public String getLastCrawlTime() {
        return lastCrawlTime;
    }

    public void setLastCrawlTime(String lastCrawlTime) {
        this.lastCrawlTime = lastCrawlTime;
    }

    public Double getPoiLimitPrice() {
        return poiLimitPrice;
    }

    public void setPoiLimitPrice(Double poiLimitPrice) {
        this.poiLimitPrice = poiLimitPrice;
    }

    public Double getGoodsLimitPrice() {
        return goodsLimitPrice;
    }

    public void setGoodsLimitPrice(Double goodsLimitPrice) {
        this.goodsLimitPrice = goodsLimitPrice;
    }

    public String getVirtualHighReason() {
        return virtualHighReason;
    }

    public void setVirtualHighReason(String virtualHighReason) {
        this.virtualHighReason = virtualHighReason;
    }

    public Integer getVirtualHighType() {
        return virtualHighType;
    }

    public void setVirtualHighType(Integer virtualHighType) {
        this.virtualHighType = virtualHighType;
    }
}
