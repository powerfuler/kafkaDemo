package com.kafka.kafkaDemo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.kafka.util.MQDict;


/**
 * @author djm
 * @Description 获取所有kafka所有的Topic
 * @Date 2020年4月6日
 * 
 * 通过 ZooKeeper API 获取 是官方提供的用于管理和查询 Kafka 元数据方法，相比 ZooKeeper 更加正规（比如通过它获取到的 topic 列表不包含 Kafka 内建的 topic，可以防止误操作），推荐使用。
 * 通过 AdminClient API 获取 API 在 0.11.0 之后的版本才有（参考 KIP-117），因此，低版本的 Kafka 可能只能通过 ZooKeeper 的 API 来进行相关的操作。
 * 
 */

public class getAllTopic {
	public static void main(String[] args) {
		try {
			zookeeperListTopics();
		} catch (IOException | KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			kafkaListTopics();
		} catch (ExecutionException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	//  ZooKeeper方式
	public static void zookeeperListTopics() throws IOException, KeeperException, InterruptedException {
	    // 创建 ZooKeeper 对象，三个参数分别是 ZK 的 URL、超时时间和一个 watcher 对象，在这里将 watcher 对象设为空即可
	    ZooKeeper zk = new ZooKeeper(MQDict.ZK_ADDRESS_COLLECTION, MQDict.SESSION_TIMEOUT, null);
	    // 查看 /brokers/topics 的子节点
	    List<String> topics = zk.getChildren("/brokers/topics", false);
	    System.out.println(topics);
	}
	
	// AdminClient方式
	public static void kafkaListTopics() throws ExecutionException, InterruptedException {
	    Properties props = new Properties();
	    // 只需要提供一个或多个 broker 的 IP 和端口
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MQDict.MQ_ADDRESS_COLLECTION);
	    // 创建 AdminClient 对象
	    AdminClient client = KafkaAdminClient.create(props);
	    // 获取 topic 列表
	    Set topics = client.listTopics().names().get();
	    System.out.println(topics);
	}

}
