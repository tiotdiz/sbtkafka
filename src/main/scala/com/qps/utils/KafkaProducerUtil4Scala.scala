package com.qps.utils

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * scala实现kafka producer工具类
 */

object KafkaProducerUtil4Scala {
  //将生产者对象的获取封装到方法中
  def getKafkaProducer(brokersList: String): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersList)
    //key序列化
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())
		//value序列化
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())
		var producer4Kafka = new KafkaProducer[String, String](properties)
		producer4Kafka
		
  }
  
  def main(args: Array[String]): Unit = {
      //定义broker list，topic
      val brokersList = "sc-slave7:6667,sc-slave8:6667"
      val topic: String = "TestKafkaInScala"
      //获取生产者对象
      var producer4Scala = KafkaProducerUtil4Scala.getKafkaProducer(brokersList)
      //发送实际的message
      producer4Scala.send(new ProducerRecord(topic, "heelowrold"))
      //发送完关闭
      producer4Scala.close()
      println("Done!!")
    }
  
  
}