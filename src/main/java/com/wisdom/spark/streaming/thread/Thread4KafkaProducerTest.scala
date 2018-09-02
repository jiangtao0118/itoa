package com.wisdom.spark.streaming.thread

import com.wisdom.spark.streaming.tools.{DateUtil, KafkaProducerUtil, KerbInit}

/**
  * Created by zhengz on 2016/12/14.
  * 该类包含一个主函数用于测试kafka消息发送
  */
object Thread4KafkaProducerTest {
  def main(args: Array[String]) {
    KerbInit.init()
    val message = "TestMessage"
    val producer = new KafkaProducerUtil()
    for (i <- 1 to 10000) {
      if (i % 5 == 0) {
        Thread.sleep(2000)
      }
      producer.kafkaSend("******* " + DateUtil.getCurrentTimeWithFormat + " " + message + i + " ******* ")
    }
  }
}
