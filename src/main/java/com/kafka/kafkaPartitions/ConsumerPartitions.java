package com.kafka.kafkaPartitions;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description kafka自定义分区的使用
 * @Date 2020年4月6日
 */

public class ConsumerPartitions {

	public static void main(String[] args) {
		// 获取消费者,实例化consumer
		KafkaConsumer<String, String> consumer = MQDict.getKafkaConsumer();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				if (consumer != null) {
					consumer.close();
				}
			}
		}));

		TopicPartition pp = new TopicPartition(MQDict.CONSUMER_TOPIC, 1);

		// 这个是指定分区消费
		consumer.assign(Arrays.asList(pp));
		//consumer.seekToBeginning(Arrays.asList(pp));// 重头开始消费
		// consumer.seek(pp,5);//指定从topic的分区的某个offset开始消费

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(MQDict.CONSUMER_POLL_SECOND_OUT);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.topic() + "---" + record.value());
			}
		}
	}
}
