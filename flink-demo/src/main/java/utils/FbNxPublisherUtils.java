package utils;

import com.alibaba.fastjson.JSONObject;
import com.sankuai.xm.pub.push.Pusher;
import com.sankuai.xm.pub.push.PusherBuilder;
import com.sankuai.xm.pub.vo.TextMessage;
import com.sankuai.xm.pub.vo.XBody;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Author: mt-chengyuxing
 * Date: 2017/4/1
 * Time: 下午2:30
 * Desc: 应用号【飞豹系统】发送消息
 */
public class FbNxPublisherUtils {

    private static Pusher pusher;

    private static Map<FbNxPublisherEnum, Pusher> pusherMaps = new HashMap<>();

    private static final String appKey = "12701338t1320021";

    private static final String appToken = "532564aca81a5d6b484c9bb7e6a075dc";
    private static final String url = "https://xmapi.vip.sankuai.com/api/pub/push";
    private static final long fromUid = 137438958797L;

    private static final Logger logger = LoggerFactory.getLogger(FbNxPublisherUtils.class);

    static {

        pusher = PusherBuilder.defaultBuilder().withAppkey(appKey).withApptoken(appToken)
                .withTargetUrl(url).withFromUid(fromUid).build();

        for (FbNxPublisherEnum fbNxPublisherEnum : FbNxPublisherEnum.values()) {
            Pusher temp = PusherBuilder.defaultBuilder().withAppkey(fbNxPublisherEnum.getAppKey()).withApptoken(fbNxPublisherEnum.getAppSecret())
                    .withTargetUrl(url).withFromUid(fbNxPublisherEnum.getPubID()).build();
            pusherMaps.put(fbNxPublisherEnum, temp);
        }
    }

    private FbNxPublisherUtils() {

    }

    /**
     * 飞豹系统主公众号,推送消息
     *
     * @param title
     * @param content
     * @param receivers
     * @return
     */
    public static JSONObject sendText(String title, String content, String... receivers) {
        TextContentDTO textContentDTO = genTextContentDTO(title, content);
        return sendText(textContentDTO, receivers);
    }

    public static JSONObject sendText(TextContentDTO textContentDTO, String[] receivers) {
        TextMessage textMessage = genTextMessage(textContentDTO);
        return send(textMessage, receivers);
    }

    private static TextContentDTO genTextContentDTO(String title, String content) {
        TextContentDTO textContentDTO = new TextContentDTO();
        textContentDTO.setTitle(title.length() > 5000 ? title.substring(0, 4999) + "......" : title);
        textContentDTO.setContent(content.length() > 5000 ? content.substring(0, 4999) + "......" : content);
        return textContentDTO;
    }

    private static TextMessage genTextMessage(TextContentDTO textContentDTO) {
        if (null == textContentDTO) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(textContentDTO.getTitle())) {
            sb.append(textContentDTO.getTitle());
            sb.append("\n");
        }

        sb.append(textContentDTO.getContent());

        if (MapUtils.isNotEmpty(textContentDTO.getLinks())) {
            for (Map.Entry<String, String> entry : textContentDTO.getLinks().entrySet()) {
                String name = entry.getKey();
                String url = entry.getValue();

                sb.append("\n").append("[").append(name).append("|").append(url).append("]");
            }
        }

        if (textContentDTO.getTail() != null) {
            sb.append("\n").append(textContentDTO.getTail());
        }

        TextMessage textMessage = new TextMessage();
        textMessage.setText(sb.toString());
        textMessage.setBold(false);
        textMessage.setCipherType(TextMessage.CipherType.NO_CIPHER);
        textMessage.setFontName("宋体");
        textMessage.setFontSize(18);

        return textMessage;
    }


    public static JSONObject send(XBody body, String[] receivers) {
        JSONObject result = pusher.push(body, receivers);
        return result;
    }
}
