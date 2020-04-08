package com.kafka.kafkaPartitions;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description kafka自定义分区的使用
 * @Date 2020年4月6日
 */
public class ProducerPartitions {

	public static void main(String[] args) {
		Producer<Object, String> producer = MQDict.getKafkaProducerPartitioner();

		// 无key消息
		ProducerRecord<Object, String> nonKeyRecord = new ProducerRecord<Object, String>(MQDict.PRODUCER_TOPIC, "non-key record");

		// love消息
		ProducerRecord<Object, String> loveRecord = new ProducerRecord<Object, String>(MQDict.PRODUCER_TOPIC, "love", "loveTest");

		// 非love消息
		ProducerRecord<Object, String> nonLoveRecord = new ProducerRecord<Object, String>(MQDict.PRODUCER_TOPIC, "other", "non-love record");

		// 生产消息
		try {
			// 发送无key消息
			//RecordMetadata recordMetadata1 = producer.send(nonKeyRecord).get();
			//System.out.println(recordMetadata1.toString());

			// 发送key不为love的消息
			RecordMetadata recordMetadata11 = producer.send(nonLoveRecord).get();
			System.out.println(recordMetadata11.toString());

			// 发送key为love的消息
			RecordMetadata recordMetadata21 = producer.send(loveRecord).get();
			System.out.println(recordMetadata21.toString());

			// 发送无key消息
			//RecordMetadata recordMetadata11 = producer.send(nonKeyRecord).get();
			//System.out.println(recordMetadata11.toString());

			// 发送key不为love的消息
			RecordMetadata recordMetadata12 = producer.send(nonLoveRecord).get();
			System.out.println(recordMetadata12.toString());
			
			// 发送key不为love的消息
			RecordMetadata recordMetadata22 = producer.send(loveRecord).get();
			System.out.println(recordMetadata22.toString());
			
			// 发送key不为love的消息
			RecordMetadata recordMetadata23 = producer.send(loveRecord).get();
			System.out.println(recordMetadata23.toString());
			
			// 发送key不为love的消息
			RecordMetadata recordMetadata13 = producer.send(nonLoveRecord).get();
			System.out.println(recordMetadata13.toString());
			

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