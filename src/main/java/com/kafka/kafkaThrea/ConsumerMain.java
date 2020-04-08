package com.kafka.kafkaThrea;

import com.kafka.util.MQDict;

/**
 * 
 * @Description TODO
 *
 */
public class ConsumerMain {

    public static void main(String[] args) {
        // kafka即broker列表
        String brokerList = MQDict.MQ_ADDRESS_COLLECTION;
        // group组名称
        String groupId = "group2";
        // topic主题名称
        String topic = MQDict.PRODUCER_TOPIC;
        // 消费者的数量
        int consumerNum = 3;
        // 通过构造器创建出一个对象
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        // 执行execute的方法，创建出ConsumerRunnable消费者实例。多线程多消费者实例
        consumerGroup.execute();
    }

}