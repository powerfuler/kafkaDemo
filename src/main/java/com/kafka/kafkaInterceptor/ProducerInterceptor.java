package com.kafka.kafkaInterceptor;


import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description 拦截器链的使用
 * @Date 2020年4月6日
 */
public class ProducerInterceptor {

    public static void main(String[] args) {

        Producer<String, String> producer = MQDict.getKafkaProducerInterceptor();
        
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(MQDict.PRODUCER_TOPIC, "key" + Integer.toString(i), "value" + Integer.toString(i));
            try {
                RecordMetadata recordMetadata = producer.send(record).get();
                System.out.println("topic : " + recordMetadata.topic() + ", partition" + recordMetadata.partition());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.println("消息生产结束......");
        // 关闭kafkaProduce对象
        producer.close();
        System.out.println("关闭生产者......");
    }

}