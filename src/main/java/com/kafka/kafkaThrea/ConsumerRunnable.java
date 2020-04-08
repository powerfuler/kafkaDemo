package com.kafka.kafkaThrea;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 
 * @Description TODO
 *       1、KafkaConsumer是非线程安全的，KafkaProducer是线程安全的。
 *       2、该案例是每个线程维护一个KafkaConsumer实例
 *       用户创建多个线程消费topic数据，每个线程都会创建专属该线程的KafkaConsumer实例
 *       3、ConsumerRunnable，消费线程类，执行真正的消费任务
 */
public class ConsumerRunnable implements Runnable {

    // 每个线程维护私有的kafkaConsumer实例
    private final KafkaConsumer<String, String> consumer;

    /**
     * 默认每个消费者的配置参数初始化
     * 
     * @param brokerList
     * @param groupId
     * @param topic
     */
    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        // 带参数的构造方法
        Properties props = new Properties();
        // kafka的列表
        props.put("bootstrap.servers", brokerList);
        // 消费者组编号
        props.put("group.id", groupId);
        // 自动提交
        props.put("enable.auto.commit", true);
        // 提交提交每个一秒钟
        props.put("auto.commit.interval.ms", "1000");
        // 反序列化key
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 反序列化value
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 将配置信息进行初始化操作
        this.consumer = new KafkaConsumer<>(props);
        // 定义响应的主题信息topic
        consumer.subscribe(Arrays.asList(topic));
    }

    /**
     * 
     */
    @Override
    public void run() {
        // 消费者保持一直消费的状态
        while (true) {
            // 将获取到消费的信息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(200));
            // 遍历出每个消费的消息
            for (ConsumerRecord<String, String> record : records) {
                // 输出打印消息
                System.out.println(
                        "当前线程名称 : " + Thread.currentThread().getName() + ", 主题名称 :" + record.topic() + ", 分区名称 :"
                                + record.partition() + ", 位移名称 :" + record.offset() + ", value :" + record.value());
            }
        }
    }

}