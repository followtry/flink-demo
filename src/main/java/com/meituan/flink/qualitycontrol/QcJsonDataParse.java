package com.meituan.flink.qualitycontrol;

import com.alibaba.fastjson.JSONObject;
import com.meituan.hotel.oe.flink.util.JsonParseHelper;
import com.meituan.mafka.client.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

/**
 * Created by lmalds on 2017/11/9.
 * 解析Json
 * <p>
 * Json格式如下：
 */
public class QcJsonDataParse implements MapFunction<String, QualityControlResultMq> {

    @Override
    public QualityControlResultMq map(String s) {
        String jsonString = JsonParseHelper.pureJsonString(s);
        Date now = new Date();
        if (!StringUtils.startsWith(jsonString, "{") || !StringUtils.endsWith(jsonString, "}")) {
            System.out.println("time:"+ DateUtils.formatSS(now) +" json error: " + jsonString);
        }
        try {
            QualityControlResultMq mq = JSONObject.parseObject(jsonString, QualityControlResultMq.class);
            return mq;
        } catch (Exception e) {
            System.out.println("time:"+ DateUtils.formatSS(now) +" parse json error: " + e);
            System.out.println("time:"+ DateUtils.formatSS(now) +" parse json input: " + jsonString);
            System.out.println("time:"+ DateUtils.formatSS(now) +" origin json input: " + s);
        }
        return new QualityControlResultMq();
    }



}