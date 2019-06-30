package cn.followtry.app;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/27
 */
public class UserInfo implements Serializable{

    /**  */
    private Long eventTime;

    private String eventTimeStr;

    private String name;

    private long id;

    public UserInfo() {
    }

    public UserInfo(Long eventTime, String name, long id) {
        this.eventTime = eventTime;
        this.eventTimeStr = DateFormatUtils.format(eventTime,"yyyy-MM-dd HH:mm:ss:SSS");
        this.name = name;
        this.id = id;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getEventTimeStr() {
        return eventTimeStr;
    }

    public void setEventTimeStr(String eventTimeStr) {
        this.eventTimeStr = eventTimeStr;
    }

    public String getKey(String keyName){
        if (Objects.equals("eventTime", keyName)) {
            return this.eventTimeStr;
        } else {
            return this.name;
        }
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "eventTime=" + eventTime +
                ", eventTimeStr='" + eventTimeStr + '\'' +
                ", id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
