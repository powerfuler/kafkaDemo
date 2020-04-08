package com.kafka.kafkaPartitions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * @author djm
 * @Description kafka自定义分区的使用。
 * @Date 2020年4月6日 可以根据分区，将指定的key消息发送到指定的分区，或者将value消息发送到指定的分区。 自定义分区
 *       可以将指定消息发送到指定的分区
 */
public class LovePartitioner implements Partitioner {

	// 随机数
	private Random random;

	@Override
	public void configure(Map<String, ?> configs) {
		// 该方法实现必要资源的初始化工作
		random = new Random();
	}

	@Override
	public void close() {
		// 該方法实现必要的清理工作
	}

	@Override
	public int partition(String topic, Object keyObject, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// 获取到key
		String key = (String) keyObject;
		// 打印输出key信息
		System.out.println("key : " + key);
		// 获取到集群的元数据信息
		List<PartitionInfo> partitionsForTopic = cluster.availablePartitionsForTopic(topic);
		// 遍历分区元数据信息
		Iterator<PartitionInfo> it = partitionsForTopic.iterator();
		while (it.hasNext()) {
			PartitionInfo partitionInfo = it.next();
			System.out.println("topic消息信息： " + partitionInfo.topic() + " , partition分区信息： " + partitionInfo.partition()
					+ " ,leader信息： " + partitionInfo.leader() + " , replicas备份信息： " + partitionInfo.replicas());
		}
		// 获取到分区的数量
		int partitionCount = partitionsForTopic.size();
		System.out.println("获取到分区的数量 : " + partitionCount);
		// 获取到最后一个分区
		int lovePartition = partitionCount - 1;
		System.out.println("获取到最后一个分区 : " + lovePartition);
		
		// 如果key不为空且不是love消息，就将随机分发到除最后一个分区的其他分区，否则，分发到最后一个分区。
		boolean b = (key == null || key.isEmpty() || !key.contains("love"));
		
		int i = b ? random.nextInt(partitionCount - 1) : lovePartition;
		
		System.out.println("该key分配到的分区 : " + i);
		
		return i;
	}

}