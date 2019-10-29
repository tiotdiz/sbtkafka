package com.qps.utils

import java.util.Properties 
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig 
import org.apache.kafka.clients.consumer.KafkaConsumer 
import java.util.Collections 
import org.apache.kafka.clients.consumer.ConsumerConfig 
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * scala实现kafka consumer工具类
 */

object KafkaConsumerUtil {
  //将消费者对象的获取封装到方法中，注意groupid是必选项，此为与java api不相同之处
  def getKafkaConsumer(brokersList: String, topic: String, consumerGroupId: String): KafkaConsumer[String, String] = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersList)
    //key反序列化
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
		//value反序列化
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
		
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
		
		var Consumer4Kafka = new KafkaConsumer[String, String](properties)
		Consumer4Kafka.subscribe(Collections.singletonList(topic))
		
		Consumer4Kafka
  }
  
  def main(args: Array[String]): Unit = {
    //定义broker list
    val brokersList = "sc-slave7:6667,sc-slave8:6667"
    //指定消费者组id
    var consumerGroupId = "TestConsumerID"
    val topic: String = "TestKafkaInScala"
    
    var consumer4Kafka = KafkaConsumerUtil.getKafkaConsumer(brokersList, topic, consumerGroupId)
    //标志位循环判断
    var runnable = true
    while(runnable){
      //版本原因，此处不用for循环，而用iterator遍历
      val records = consumer4Kafka.poll(1000)
      var iter = records.iterator()
      while(iter.hasNext()){
        val record = iter.next()
        println(record.offset() + "---" + record.key() + "---" + record.value())
      }
    }
    consumer4Kafka.close()
    println("Done!")
  }
}