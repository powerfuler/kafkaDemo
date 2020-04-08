package com.kafka.kafkaWorker;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 
 * @Description TODO
 *            1、本质上是一个Runnable，执行真正的消费逻辑并且上报位移信息给ConsumerThreadHandler。
 * 
 */
public class ConsumerWorker<K, V> implements Runnable {

    // 获取到的消息
    private final ConsumerRecords<K, V> records;
    // 位移信息
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * ConsumerWorker有参构造方法
     * 
     * @param records
     *            获取到的消息
     * @param offsets
     *            位移信息
     */
    public ConsumerWorker(ConsumerRecords<K, V> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    /**
     * 
     */
    @Override
    public void run() {
        // 获取到分区的信息
        for (TopicPartition partition : records.partitions()) {
            // 获取到分区的消息记录
            List<ConsumerRecord<K, V>> partConsumerRecords = records.records(partition);
            // 遍历获取到的消息记录
            for (ConsumerRecord<K, V> record : partConsumerRecords) {
                // 打印消息
                System.out.println(Thread.currentThread().getName() + "topic: " + record.topic() + ",partition: " + record.partition() + ",offset: "
                        + record.offset() 
                        + ",消息记录: " + record.value());
            }
            // 上报位移信息。获取到最后的位移消息，由于位移消息从0开始，所以最后位移减一获取到位移位置
            long lastOffset = partConsumerRecords.get(partConsumerRecords.size() - 1).offset();
            // 同步锁，锁住offsets位移
            synchronized (offsets) {
                // 如果offsets位移不包含partition这个key信息
                if (!offsets.containsKey(partition)) {
                    // 就将位移信息设置到map集合里面
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                } else {
                    // 否则，offsets位移包含partition这个key信息
                    // 获取到offsets的位置信息
                    long curr = offsets.get(partition).offset();
                    // 如果获取到的位置信息小于等于上一次位移信息大小
                    if (curr <= lastOffset + 1) {
                        // 将这个partition的位置信息设置到map集合中。并保存到broker中。
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
    }

}