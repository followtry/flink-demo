package cn.followtry.app;

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
