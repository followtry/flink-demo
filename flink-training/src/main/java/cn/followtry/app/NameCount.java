package cn.followtry.app;

import java.util.List;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/28
 */
public class NameCount{

    private String name;

    /**  */
    private Integer count;

    /**  */
    private String startTime;

    private String endTime;

    /**  */
    private List<UserInfo> detailItems;

    public NameCount() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public List<UserInfo> getDetailItems() {
        return detailItems;
    }

    public void setDetailItems(List<UserInfo> detailItems) {
        this.detailItems = detailItems;
    }

    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", endTime='" + endTime + '\'' +
                ", name='" + name + '\'' +
                ", startTime='" + startTime + '\'' +
                '}';
    }
}
