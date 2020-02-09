package utils;

/**
 * Created with IntelliJ IDEA.
 * Author: mt-chengyuxing
 * Date: 2017/10/13
 * Time: 下午5:23
 * Desc: 所有的公众号的信息,都放在枚举中。避免每个引入common的项目,都自己维护一份公众号的配置。
 */
public enum FbNxPublisherEnum {

    FLY_LEOPRARD(137438958797L,"12701338t1320021","532564aca81a5d6b484c9bb7e6a075dc","飞豹系统"),
    LEOPARD_ROOM_STATUS(137440759167L,"71321u0118478201","87588c1372c3845023f32ffea95aad5d","飞豹房态竞争力"),
    LEOPARD_DEEP(137440760125L,"120180113g487017","9e4cafb01627ed23c989b3cacc42ba2c","飞豹deep"),
    LEOPARD_GOODS_CTRL(137440760163L,"32091101v810107f","b6af5c442440cd29fbe1ccd11ed67cda","飞豹质控"),
    LEOPARD_DOWN(137440862774L, "212100W1d7154250", "35a3f7ac2388aff4ad282347c60c8a62", "飞豹下载助手"),
    OPERATE_RECORD(137441252825L,"121T7W4041214122","72f2719def2e9bd2e14f583e5e54ac83","飞豹操作记录提醒"),
    RAISE_BEE(137441333148L, "0972105112k32W14", "12b12e9c182e7b8a6dbb1b19dbdc31dc", "飞豹养蜂"),
    BUSINESS_NOTICE(137442239638L,"10T8090471152223","30ae94eb3bc142beac93601ab622e0d9","飞豹业务通知");
    private Long pubID;

    private String appKey;

    private String appSecret;

    private String desc;

    FbNxPublisherEnum(Long pubID, String appKey, String appSecret, String desc) {
        this.pubID = pubID;
        this.appKey = appKey;
        this.appSecret = appSecret;
        this.desc = desc;
    }

    public Long getPubID() {
        return pubID;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getAppSecret() {
        return appSecret;
    }

    public String getDesc() {
        return desc;
    }
}
