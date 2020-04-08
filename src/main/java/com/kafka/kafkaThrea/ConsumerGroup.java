package com.kafka.kafkaThrea;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @Description TODO
 *       1、消费线程管理类，创建多个线程类执行消费任务
 */
public class ConsumerGroup {

    // 消费者群组，多消费者。
    private List<ConsumerRunnable> consumers;

    /**
     * 
     * @param consumerNum
     * @param groupId
     * @param topic
     * @param brokerList
     */
    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        // 初始化消费者组
        consumers = new ArrayList<>(consumerNum);
        // 初始化消费者，创建多少个消费者
        for (int i = 0; i < consumerNum; i++) {
            // 根据消费者构造方法，创建消费者实例
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(brokerList, groupId, topic);
            // 将创建的消费者实例添加到消费者组中
            consumers.add(consumerRunnable);
        }
    }

    /**
     * 
     */
    public void execute() {
        // 将消费者组里面的消费者遍历出来
        for (ConsumerRunnable task : consumers) {
            // 创建一个消费者线程，并且启动该线程
            new Thread(task).start();
        }
    }

}