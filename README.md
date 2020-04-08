# kafkaDemo
此项目是熟悉kafka工具以及用Java调用kafkaApi进行消息的发送与消费的测试案例。

# 项目特性
1、kafka生产者、消费者简单demo，Java调用。

2、kafka同步、异步调用。

3、kafka自定义分区的使用。

4、kafka自定义指定序列化生产的消息。

5、kafka拦截器链的使用。

6、kafka消费者消费消息之每个线程维护一个KafkaConsumer实例，用户创建多个线程消费topic数据，每个线程都会创建专属该线程的KafkaConsumer实例。

7、单独KafkaConsumer实例和多worker线程。使用全局的KafkaConsumer实例执行消息获取，然后把获取到的消息集合交给线程池中的worker线程执行工作，之后worker线程完成处理后上报位移状态，由全局consumer提交位移。

8、获取所有kafka所有的Topic。

9、获取当前topic每个分区内最新的30条消息(如果topic额分区内有没有30条，就获取实际消息)。
