package utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author jingzhongzhi
 * @since 2018-12-17
 */
public class JsonParseHelper {

    public static String pureJsonString(String srcContent){

        String newJsonStr = StringUtils.strip(srcContent);
        int startIndex = findJsonStartIndex(newJsonStr);
        String jsonString;
        if (startIndex >= 0) {
            jsonString = newJsonStr.substring(startIndex);
        }else{
            jsonString = newJsonStr;
        }
        return jsonString;
    }

    private static int findJsonStartIndex(String jsonStr){
        int startIndex = -1;
        String newJson = StringUtils.strip(jsonStr);
        int startA = newJson.indexOf("{\"a");
        int startB = newJson.indexOf("{\"b");
        boolean startDoubleBrace = newJson.startsWith("{\"{\"");
        if (startDoubleBrace){
            System.out.println("原始消息以两个花括号开头： " + jsonStr);
            startIndex = 2;
        }else if (startA >= 0){
            startIndex = startA;
        } else if (startB >= 0){
            startIndex = startB;
        } else {
            //其他类型
        }
        return startIndex;
    }
}
