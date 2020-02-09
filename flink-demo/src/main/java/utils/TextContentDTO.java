package utils;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Author: mt-chengyuxing
 * Date: 2017/4/10
 * Time: 上午11:48
 * Desc:
 * 【title】[P2][恢复正常]
 【content】主机名: dx-hotel-cbs-pegasuscollect02
 监控项:  all(#10) mem.swapused.percent  >= 80
 当前值: 8.56366
 业务负责人: taoruzhi,chengyuxing
 告警模板: MEITUAN_BASE_ALL
 告警次数: 第1次
 时间: 2017-04-08 15:55:00
 【links】[调用链][dashboard]
 【tail】。。。
 */
public class TextContentDTO {
    private String title;
    private String content;
    private Map<String, String> links;
    private String tail;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Map<String, String> getLinks() {
        return links;
    }

    public void setLinks(Map<String, String> links) {
        this.links = links;
    }

    public String getTail() {
        return tail;
    }

    public void setTail(String tail) {
        this.tail = tail;
    }
}

