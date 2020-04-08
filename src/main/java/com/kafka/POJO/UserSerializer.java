package com.kafka.POJO;


import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author djm
 * @Description 自定义序列化操作
 * @Date 2020年4月6日
 */
public class UserSerializer implements Serializer<Object> {

    private Logger logger = Logger.getLogger(UserSerializer.class);

    //
    private ObjectMapper objectMapper;

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(data).getBytes("utf-8");
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("自定义序列化失败告警.....请紧急处理", e);
        }
        return ret;
    }

}