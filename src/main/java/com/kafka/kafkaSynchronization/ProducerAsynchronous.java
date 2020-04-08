package com.kafka.kafkaSynchronization;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description KafKa生成者 异步发送
 * @Date 2020年4月6日 Asynchronous 异步
 */
public class ProducerAsynchronous {

	public static void main(String[] args) {
		// 获取生产者
		Producer<String, String> producer = MQDict.getKafkaProducer();
		for (int i = 0; i < 100; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(MQDict.PRODUCER_TOPIC, "key" + Integer.toString(i), "value" + Integer.toString(i));
			// 异步发送
			producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						// exception == null代表消息发送成功
						System.out.println("消息发送成功......");
					} else {
						// 消息发送失败，执行错误的逻辑
						System.out.println("消息发送失败......");
						if (exception instanceof RetriableException) {
							// 处理可重试瞬时异常
							// ...
						} else {
							// 处理不可重试异常
							// ...
						}

					}
				}
			});
		}
		System.out.println("消息生产结束......");
		// 关闭kafkaProduce对象
		producer.close();
		System.out.println("关闭生产者......");
	}

}