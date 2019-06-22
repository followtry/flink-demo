package com.meituan.flink.qualitycontrol;

import java.io.Serializable;
import java.util.List;

/**
 * @author jingzhongzhi
 * @since 2018/3/11
 */
public class QualityControlResultMq implements Serializable{

    /**
     * 质控类型，1.实时质控
     */
    private Integer gcType;

    private List<GcResult> results;

    private String clientAppKey;

    private String clientIp;

    private String clientRequestparam;

    public Integer getGcType() {
        return gcType;
    }

    public void setGcType(Integer gcType) {
        this.gcType = gcType;
    }

    public List<GcResult> getResults() {
        return results;
    }

    public void setResults(List<GcResult> results) {
        this.results = results;
    }

    public String getClientAppKey() {
        return clientAppKey;
    }

    public void setClientAppKey(String clientAppKey) {
        this.clientAppKey = clientAppKey;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getClientRequestparam() {
        return clientRequestparam;
    }

    public void setClientRequestparam(String clientRequestparam) {
        this.clientRequestparam = clientRequestparam;
    }
}
