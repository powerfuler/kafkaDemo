package com.kafka.kafkaSimpleDemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description KafKa消息消费
 * @Date 2020年4月6日
 */
public class ConsumerSimpleDemo {

	public static void main(String[] args) {
		// 获取消费者
		KafkaConsumer<String, String> consumer = MQDict.getKafkaConsumer();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(MQDict.CONSUMER_POLL_SECOND_OUT); // 拉取消息，阻塞时间5秒

			if (records.isEmpty())
				break;
			// 遍历消息并打印value
			records.forEach(rec -> System.out
					.println("主题topic:" + rec.topic() + "; topickey:" + rec.key() + "; topicval:" + rec.value()));

			
			/*for (ConsumerRecord<String, String> record : records) {
				// 简单的打印输出
				System.out.println(
						"offset = " + record.offset() + ",key = " + record.key() + ",value =" + record.value());
			}*/
		}
		// 关闭消费者
		consumer.close();
	}

}