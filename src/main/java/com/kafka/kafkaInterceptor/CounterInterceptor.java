package com.kafka.kafkaInterceptor;


import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author djm
 * @Description
 * @Date 2020年4月6日
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int errorCount = 0;// 失败的消息数量
    private int successCount = 0;// 成功的消息数量

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        return record;
    }

    @Override
    public void configure(Map<String, ?> arg0) {

    }

    @Override
    public void close() {
        //
        System.out.println("success count is : " + successCount);
        System.out.println("error count is : " + errorCount);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCount++;
        } else {
            errorCount++;
        }
    }

}