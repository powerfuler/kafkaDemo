package com.kafka.kafkaSynchronization;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description KafKa生成者 同步发送
 * @Date 2020年4月6日 Synchronization 同步
 */
public class ProducerSynchronization {

	public static void main(String[] args) {
		// 获取生产者
		Producer<String, String> producer = MQDict.getKafkaProducer();
		for (int i = 0; i < 100; i++) {
			// 构造好kafkaProducer实例以后，下一步就是构造消息实例。
			// producer.send(new ProducerRecord<>("topic1", Integer.toString(i), Integer.toString(i)));

			try {
				Future<RecordMetadata> future = producer.send(new ProducerRecord<>(MQDict.PRODUCER_TOPIC, "key" + Integer.toString(i), "value" + Integer.toString(i)));
				// 同步发送，调用get()方法无限等待返回结果
				RecordMetadata recordMetadata = future.get();
				// 成功返回RecordMetadata实例（包含发送的元数据信息）
				System.out.println("第 " + i + " 条, " + recordMetadata.toString() +" ; key" +recordMetadata.serializedKeySize()+"; value " +recordMetadata.serializedValueSize());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println("消息生产结束......");
		// 关闭kafkaProduce对象
		producer.close();
		System.out.println("关闭生产者......");
	}

}