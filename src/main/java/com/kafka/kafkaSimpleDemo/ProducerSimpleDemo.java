package com.kafka.kafkaSimpleDemo;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.util.MQDict;

/**
 * @author djm
 * @Description KafKa生成者
 * @Date 2020年4月6日
 */
public class ProducerSimpleDemo {

	public static void main(String[] args) {
		// 获取生产者
		Producer<String, String> producer = MQDict.getKafkaProducer();

		for (int i = 0; i < 500; i++) {
			// 方式1
			// 构造好kafkaProducer实例以后，下一步就是构造消息实例。生成 ProducerRecord 对象，并制定 Topic，key 以及 value
			// producer.send(new ProducerRecord<>(MQDict.PRODUCER_TOPIC, "key" + Integer.toString(i), "value" + Integer.toString(i)));
			
			// 方式2
			// 构造待发送的消息对象ProduceRecord的对象，指定消息要发送到的topic主题，分区以及对应的key和value键值对。 注意，分区和key信息可以不用指定，由kafka自行确定目标分区。
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(MQDict.PRODUCER_TOPIC, "key" + Integer.toString(i), "value" + Integer.toString(i));
			// 调用kafkaProduce的send方法发送消息
			producer.send(producerRecord);
		}
		System.out.println("消息生产结束......");
		// 关闭kafkaProduce对象
		producer.close();
		System.out.println("关闭生产者......");
	}

}