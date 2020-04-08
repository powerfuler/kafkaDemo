package com.kafka.kafkaWorker;

import com.kafka.util.MQDict;

/**
 * 
 * @Description TODO
 *       1、单独KafkaConsumer实例和多worker线程。
 *       2、将获取的消息和消息的处理解耦，将消息的处理放入单独的工作者线程中，即工作线程中，
 *       同时维护一个或者若各干consumer实例执行消息获取任务。
 *       3、本例使用全局的KafkaConsumer实例执行消息获取，然后把获取到的消息集合交给线程池中的worker线程执行工作，
 *       之后worker线程完成处理后上报位移状态，由全局consumer提交位移。
 * 
 * 
 */

public class ConsumerMain {

	public static void main(String[] args) {
		// broker列表
		String brokerList = MQDict.MQ_ADDRESS_COLLECTION;
		// 主题信息topic
		String topic = MQDict.PRODUCER_TOPIC;
		// 消费者组信息group
		String groupId = "group2";
		// 根据ConsumerThreadHandler构造方法构造出消费者
		final ConsumerThreadHandler<byte[], byte[]> handler = new ConsumerThreadHandler<>(brokerList, groupId, topic);
		final int cpuCount = Runtime.getRuntime().availableProcessors();
		System.out.println("cpuCount : " + cpuCount);
		
		// 创建线程的匿名内部类
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				// 执行consume，在此线程中执行消费者消费消息。
				handler.consume(cpuCount);
			}
		};
		// 直接调用runnable此线程，并运行
		new Thread(runnable).start();

		try {
			// 此线程休眠20000
			Thread.sleep(20000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Starting to close the consumer...");
		// 关闭消费者
		handler.close();
	}

}