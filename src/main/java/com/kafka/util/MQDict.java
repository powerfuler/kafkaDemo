package com.kafka.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.kafka.POJO.User;

/**
 * @author djm
 * @Description KafKa公共参数配置
 * @Date 2020年4月6日
 */
public class MQDict {
	public static final String MQ_ADDRESS_COLLECTION = "192.168.20.112:9092,192.168.20.112:9093,192.168.20.112:9094"; // kafka地址
	public static final String ZK_ADDRESS_COLLECTION = "192.168.20.112:2181"; // ZK地址
	public static final String CONSUMER_TOPIC = "SimpleDemo6"; // 消费者连接的topic
	public static final String PRODUCER_TOPIC = "SimpleDemo6"; // 生产者连接的topic
	public static final String CONSUMER_GROUP_ID = "group.1"; // groupId，可以分开配置
	public static final String CONSUMER_ENABLE_AUTO_COMMIT = "true"; // 是否自动提交（消费者）
	public static final String CONSUMER_AUTO_COMMIT_INTERVAL_MS = "1000";
	public static final String CONSUMER_SESSION_TIMEOUT_MS = "30000"; // 连接超时时间
	public static final int CONSUMER_MAX_POLL_RECORDS = 10; // 每次拉取数
	public static final Duration CONSUMER_POLL_TIME_OUT = Duration.ofMillis(3000); // 拉去数据超时时间
	public static final Duration CONSUMER_POLL_SECOND_OUT = Duration.ofSeconds(2); // 拉去数据超时时间
	
	public static final int SESSION_TIMEOUT = 10000; // 超时时间
	

	private static KafkaProducer<String, String> producer = null;
	private static KafkaConsumer<String, String> consumer = null;
	private static KafkaProducer<String, User> producerUser = null;

	// 初始化配置
	public static Properties initProducerConfig() {
		// 构造一个java.util.Properties对象
		Properties props = new Properties();
		// 指定bootstrap.servers属性。必填，无默认值。用于创建向kafka broker服务器的连接。
		props.put("bootstrap.servers", MQ_ADDRESS_COLLECTION);
		// acks参数用于控制producer生产消息的持久性（durability）。参数可选值，0、1、-1（all）。
		props.put("acks", "all");
		// props.put(ProducerConfig.ACKS_CONFIG, "1");

		// 在producer内部自动实现了消息重新发送。默认值0代表不进行重试。
		// props.put("retries", 3);
		props.put(ProducerConfig.RETRIES_CONFIG, 3);

		// 调优producer吞吐量和延时性能指标都有非常重要作用。默认值16384即16KB。
		props.put("batch.size", 323840);
		// props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);

		// 指定key.serializer属性。必填，无默认值。被发送到broker端的任何消息的格式都必须是字节数组。
		// 因此消息的各个组件都必须首先做序列化，然后才能发送到broker。该参数就是为消息的key做序列化只用的。
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 指定value.serializer属性。必填，无默认值。和key.serializer类似。此被用来对消息体即消息value部分做序列化。
		// 将消息value部分转换成字节数组。
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// props.put("key.serializer", StringSerializer.class.getName());
		// props.put("value.serializer", StringSerializer.class.getName());

		// 控制消息发送延时行为的，该参数默认值是0。表示消息需要被立即发送，无须关系batch是否被填满。
		props.put("linger.ms", 10);
		// props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

		// 指定了producer端用于缓存消息的缓冲区的大小，单位是字节，默认值是33554432即32M。
		props.put("buffer.memory", 33554432);
		// props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put("max.block.ms", 3000);
		// props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

		// 设置producer段是否压缩消息，默认值是none。即不压缩消息。GZIP、Snappy、LZ4
		// props.put("compression.type", "none");
		// props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		// 该参数用于控制producer发送请求的大小。producer端能够发送的最大消息大小。
		// props.put("max.request.size", 10485760);
		// props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
		// producer发送请求给broker后，broker需要在规定时间范围内将处理结果返还给producer。默认30s
		// props.put("request.timeout.ms", 60000);
		// props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

		// 使用上面创建的Properties对象构造KafkaProducer对象
		// 如果采用这种方式创建producer，那么就不需要显示的在Properties中指定key和value序列化类了呢。
		// Serializer<String> keySerializer = new StringSerializer();
		// Serializer<String> valueSerializer = new StringSerializer();
		// Producer<String, String> producer = new KafkaProducer<String,
		// String>(props, keySerializer, valueSerializer);
		
		return props;
	}

	/**
	 * 初始化消费者配置
	 */
	private static Properties initConsumerConfig() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MQ_ADDRESS_COLLECTION);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

		// 消费者是否自动提交偏移量，默认值是true,避免出现重复数据和数据丢失，可以把它设为 false。
		props.put("enable.auto.commit", CONSUMER_ENABLE_AUTO_COMMIT);

		props.put("auto.commit.interval.ms", CONSUMER_AUTO_COMMIT_INTERVAL_MS);

		// session.timeout.ms：消费者在被认为死亡之前可以与服务器断开连接的时间，默认是3s 。
		props.put("session.timeout.ms", CONSUMER_SESSION_TIMEOUT_MS);

		props.put("max.poll.records", CONSUMER_MAX_POLL_RECORDS);

		// auto.offset.reset:消费者在读取一个没有偏移量的分区或者偏移量无效的情况下的处理
		// earliest：在偏移量无效的情况下，消费者将从起始位置读取分区的记录。
		// latest：在偏移量无效的情况下，消费者将从最新位置读取分区的记录
		props.put("auto.offset.reset", "earliest");

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key反序列化器
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value反序列化器

		// max.partition.fetch.bytes：服务器从每个分区里返回给消费者的最大字节数
		// fetch.max.wait.ms:消费者等待时间，默认是500。
		// fetch.min.bytes:消费者从服务器获取记录的最小字节数。
		// max.poll.records:用于控制单次调用 call （） 方住能够返回的记录数量
		// receive.buffer.bytes和send.buffer.bytes：指定了 TCP socket
		// 接收和发送数据包的缓冲区大小，默认值为-1

		// client.id：该参数可以是任意的字符串，服务器会用它来识别消息的来源。
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo"); // ClientID
		return props;
	}

	// 获取初始化的生产者
	public static KafkaProducer getKafkaProducer() {
		Properties configs = initProducerConfig();
		producer = new KafkaProducer<String, String>(configs);
		return producer;
	}
	
	// 获取初始化的生产者
	public static KafkaProducer getKafkaProducerPartitioner() {
		Properties configs = initProducerConfig();
		//指定自定义分区
		configs.put("partitioner.class", "com.kafka.kafkaPartitions.LovePartitioner");
	    // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.kafka.kafkaPartitions.LovePartitioner");
		producer = new KafkaProducer<String, String>(configs);
		return producer;
	}
	
	// 获取初始化的生产者
	public static KafkaProducer getKafkaProducerInterceptor() {
		Properties configs = initProducerConfig();
		// 构造连接器链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.kafka.kafkaInterceptor.CounterInterceptor");
        interceptors.add("com.kafka.kafkaInterceptor.TimeStampPrependerInterceptor");
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        
		producer = new KafkaProducer<String, String>(configs);
		return producer;
	}
	
	// 获取初始化的生产者
	public static KafkaProducer getKafkaProducerUser() {
		Properties configs = initProducerConfig();
		// 自定义序列化操作
		configs.put("key.serializer", "com.kafka.POJO.UserSerializer");
        // 自定义序列化value的值
		configs.put("value.serializer", "com.kafka.POJO.UserSerializer");
		
		producerUser = new KafkaProducer<String, User>(configs);
		return producerUser;
	}

	// 获取初始化的消费者
	public static KafkaConsumer getKafkaConsumer() {
		Properties configs = initConsumerConfig();
		consumer = new KafkaConsumer<String, String>(configs);
		consumer.subscribe(Arrays.asList(MQDict.CONSUMER_TOPIC));
		return consumer;
	}
}
