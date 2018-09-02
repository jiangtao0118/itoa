package com.wisdom.spark.streaming.tools

import java.util
import java.util.Properties

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import kafka.producer.{KeyedMessage, ProducerConfig}
import kafka.javaapi.producer.Producer
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2016/12/13.
  * kafka消息发送工具类，配置kafkaProducer参数，以及重载producer的发送方法
  */
class KafkaProducerUtil extends Serializable {
  val props = ItoaPropertyUtil.getProperties()
  @transient
  val logger = Logger.getLogger(this.getClass)
  var producer: Producer[String, String] = null

  /**
    * 初始化Producer
    */
  def init(): Unit = {
    val kafkaProp = new Properties()
    kafkaProp.put("metadata.broker.list", props.getProperty("kafka.common.brokers"))
    kafkaProp.put("serializer.class", "kafka.serializer.StringEncoder");
    val conf = new ProducerConfig(kafkaProp)
    producer = new Producer[String, String](conf)
  }

  /**
    * 发送默认Topic
    *
    * @param msg
    */
  def kafkaSend(msg: String): Unit = {
    val defaultTopic = props.getProperty("kafka.topic.default")
    val finalMsg = new KeyedMessage[String, String](defaultTopic, msg)
    kafkaSend(finalMsg)
  }

  /**
    * 批量发送消息指定Topic
    *
    * @param msg
    **/
  def kafkaSend(msg: util.ArrayList[KeyedMessage[String, String]]): Unit = {
    if (producer == null) {
      init()
    }
    producer.send(msg)
  }

  /**
    * 发送消息指定Topic
    *
    * @param msg
    */
  def kafkaSend(msg: KeyedMessage[String, String]): Unit = {
    if (producer == null) {
      init()
    }
    producer.send(msg)
  }

}
