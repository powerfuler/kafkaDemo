package com.kafka.kafkaInterceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author djm
 * @Description 拦截器： 可以将拦截器组成拦截器链。
 * @Date 2020年4月6日
 */
public class TimeStampPrependerInterceptor implements ProducerInterceptor<String, String> {

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

		System.out.println("onSend::" + "topic" + record.topic() + " ;partition" + record.partition() + " ;timestamp"
				+ record.timestamp() + " ;key" + record.key() + " ;currentTimeMillis" + System.currentTimeMillis() + ","
				+ record.value().toLowerCase());

		return new ProducerRecord<String, String>(record.topic(), record.partition(), record.timestamp(), record.key(),
				System.currentTimeMillis() + "," + record.value().toLowerCase());
	}

	@Override
	public void configure(Map<String, ?> arg0) {

	}

	@Override
	public void close() {

	}

	@Override
	public void onAcknowledgement(RecordMetadata arg0, Exception arg1) {

	}

}