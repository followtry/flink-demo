package com.meituan.flink.qualitycontrol;

/**
 * @author jingzhongzhi
 * @since 2018/3/11
 */
public enum QualityControlResultEnum {

    /***/
    VIRTUAL_HIGH(1,"价格虚高");

    private Integer type;

    private String desc;

    QualityControlResultEnum(Integer type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public static QualityControlResultEnum parse(Integer type){
        switch (type) {
            case 1:
                return VIRTUAL_HIGH;
            default:
                    return null;
        }
    }

    public Integer getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }
}
