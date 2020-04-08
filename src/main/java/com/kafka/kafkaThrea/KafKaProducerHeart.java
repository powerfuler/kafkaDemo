package com.kafka.kafkaThrea;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka.util.MQDict;

/**
 * 
 * @Description TODO
 *
 */
public class KafKaProducerHeart {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", MQDict.MQ_ADDRESS_COLLECTION);
        props.put("acks", "-1");
        props.put("retries", 3);
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
        StringSerializer keySerializer = new StringSerializer();
        StringSerializer valueSerializer = new StringSerializer();
        Producer<String, String> producer = new KafkaProducer<String, String>(props, keySerializer, valueSerializer);
        String topic = MQDict.PRODUCER_TOPIC;
        String value = " hhhhhh";
        /*
         * for (int i = 0; i < 100; i++) { 
         *     //topic-key-value三元组确定消息所在位置
         *     producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),
         *     value)); 
         * }
         */

        // 异步发送
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(i), value);
            // 异步发送
            producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordmetadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("消息发送成功");
                        System.out.println(
                                "topic: " + recordmetadata.topic() + ", partition分区: " + recordmetadata.partition());
                    } else {
                        System.out.println("消息发送失败");
                    }
                }
            });
        }
        
        /*// 同步发送
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(i), value);
            try {
                RecordMetadata recordMetadata = producer.send(record).get();
                System.out.println(
                        "topic: " + recordMetadata.topic() + ", partition分区: " + recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }*/

        producer.close();
    }

}