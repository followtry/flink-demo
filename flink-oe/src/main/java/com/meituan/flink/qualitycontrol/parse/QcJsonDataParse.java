package com.meituan.flink.qualitycontrol.parse;

import com.alibaba.fastjson.JSONObject;
import com.meituan.flink.qualitycontrol.dto.QualityControlResultMq;
import com.meituan.flink.utils.JsonParseHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by lmalds on 2017/11/9.
 * 解析Json
 * <p>
 * Json格式如下：
 */
public class QcJsonDataParse implements MapFunction<String, QualityControlResultMq> {

    private static final Logger log = LoggerFactory.getLogger(QcJsonDataParse.class);

    @Override
    public QualityControlResultMq map(String s) {

        log.info("原始 mq: {}",s);

        String jsonString = JsonParseHelper.pureJsonString(s);
        Date now = new Date();
        if (!StringUtils.startsWith(jsonString, "{") || !StringUtils.endsWith(jsonString, "}")) {
            System.out.println("time:"+ now +" json error: " + jsonString);
        }
        try {
            QualityControlResultMq mq = JSONObject.parseObject(jsonString, QualityControlResultMq.class);
            return mq;
        } catch (Exception e) {
            System.out.println("time:"+ now +" parse json error: " + e);
            System.out.println("time:"+ now +" parse json input: " + jsonString);
            System.out.println("time:"+ now +" origin json input: " + s);
        }
        return new QualityControlResultMq();
    }



}