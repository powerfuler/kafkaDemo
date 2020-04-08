package com.kafka.kafkaPartitions;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.POJO.User;
import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description 自定义指定序列化生产的消息
 * @Date 2020年4月6日
 */
public class ProducerPartitions1 {
	public static void main(String[] args) {

		User user = new User(1, "张三", "41076788898765444", 25);

		Producer<String, User> producer = MQDict.getKafkaProducerUser();

		// 指定序列化生产的消息
		ProducerRecord<String, User> userRecord = new ProducerRecord<String, User>(MQDict.PRODUCER_TOPIC, user);

		// 生产消息
		try {
			// 发送无key消息
			RecordMetadata recordMetadata = producer.send(userRecord).get();
			System.out.println("topic主题：" + recordMetadata.topic() + ", partition分区" + recordMetadata.partition());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		System.out.println("消息生产结束......");
		// 关闭kafkaProduce对象
		producer.close();
		System.out.println("关闭生产者......");
	}

}