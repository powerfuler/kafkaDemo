package com.kafka.kafkaWorker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * 
 * @Description TODO
 *            1、consumer多线程管理类，用于创建线程池以及为每个线程分配消息集合。 另外consumer位移提交也在该类中完成。
 * 
 */
public class ConsumerThreadHandler<K, V> {

    // KafkaConsumer实例
    private final KafkaConsumer<K, V> consumer;
    // ExecutorService实例
    private ExecutorService executors;
    // 位移信息offsets
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    /**
     * 
     * @param brokerList
     *            kafka列表
     * @param groupId
     *            消费组groupId
     * @param topic
     *            主题topic
     */
    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        // broker列表
        props.put("bootstrap.servers", brokerList);
        // 消费者组编号Id
        props.put("group.id", groupId);
        // 非自动提交位移信息
        props.put("enable.auto.commit", "false");
        // 从最早的位移处开始消费消息
        props.put("auto.offset.reset", "earliest");
        // key反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // value反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // 将配置信息装配到消费者实例里面
        consumer = new KafkaConsumer<>(props);
        // 消费者订阅消息，并实现重平衡rebalance
        // rebalance监听器，创建一个匿名内部类。使用rebalance监听器前提是使用消费者组（consumer group）。
        // 监听器最常见用法就是手动提交位移到第三方存储以及在rebalance前后执行一些必要的审计操作。
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

            /**
             * 在coordinator开启新一轮rebalance前onPartitionsRevoked方法会被调用。
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 提交位移
                consumer.commitSync(offsets);
            }

            /**
             * rebalance完成后会调用onPartitionsAssigned方法。
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 清除位移信息
                offsets.clear();
            }
        });
    }

    /**
     * 消费主方法
     * 
     * @param threadNumber
     *            线程池中的线程数
     */
    public void consume(int threadNumber) {
        executors = new ThreadPoolExecutor(
                threadNumber, 
                threadNumber, 
                0L, 
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), 
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            // 消费者一直处于等待状态，等待消息消费
            while (true) {
                // 从主题中获取消息
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1000L));
                // 如果获取到的消息不为空
                if (!records.isEmpty()) {
                    // 将消息信息、位移信息封装到ConsumerWorker中进行提交
                    executors.submit(new ConsumerWorker<>(records, offsets));
                }
                // 调用提交位移信息、尽量降低synchronized块对offsets锁定的时间
                this.commitOffsets();
            }
        } catch (WakeupException e) {
            // 此处忽略此异常的处理.WakeupException异常是从poll方法中抛出来的异常
            //如果不忽略异常信息，此处会打印错误哦，亲
            //e.printStackTrace();
        } finally {
            // 调用提交位移信息、尽量降低synchronized块对offsets锁定的时间
            this.commitOffsets();
            // 关闭consumer
            consumer.close();
        }
    }

    /**
     * 尽量降低synchronized块对offsets锁定的时间
     */
    private void commitOffsets() {
        // 尽量降低synchronized块对offsets锁定的时间
        Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;
        // 保证线程安全、同步锁，锁住offsets
        synchronized (offsets) {
            // 判断如果offsets位移信息为空，直接返回，节省同步锁对offsets的锁定的时间
            if (offsets.isEmpty()) {
                return;
            }
            // 如果offsets位移信息不为空，将位移信息offsets放到集合中，方便同步
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            // 清除位移信息offsets
            offsets.clear();
        }
        // 将封装好的位移信息unmodfiedMap集合进行同步提交
        // 手动提交位移信息
        consumer.commitSync(unmodfiedMap);
    }

    /**
     * 关闭消费者
     */
    public void close() {
        // 在另一个线程中调用consumer.wakeup();方法来触发consume的关闭。
        // KafkaConsumer不是线程安全的，但是另外一个例外，用户可以安全的在另一个线程中调用consume.wakeup()。
        // wakeup()方法是特例，其他KafkaConsumer方法都不能同时在多线程中使用
        consumer.wakeup();
        // 关闭ExecutorService实例
        executors.shutdown();
    }

}