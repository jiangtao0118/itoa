package com.wisdom.spark.streaming.thread

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.streaming.tools.{DateUtil, KafkaProducerUtil, KerbInit, ParamUtil}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by zhengz on 2016/12/14.
  * 该类用于读取本地文件或hdfs文件，将文件通过kafka发送至topic，目的为了测试实时数据kafka消息接收逻辑是否正常
  * 包括两种文件读取方式：
  * 单个文件读取封装JSON数据发送kafka;
  * 目录读取封装JSON数据发送kafka（测试未通过）
  */
object Thread4ReadFiles {
  val props = ItoaPropertyUtil.getProperties()
  val logger = Logger.getLogger(this.getClass)

  val fileDir = props.getProperty("file.data.dir")
  val filePath = props.getProperty("file.data.fullpath")
  val itmFile = props.getProperty("data.itm.key")
  val itmBody = props.getProperty("data.itm.value")
  val tInterval = props.getProperty("unit.test.thread.interval").toInt
  val tNums = props.getProperty("unit.test.thread.nums").toInt

  val objectMapper = new ObjectMapper()

  def main(args: Array[String]) {
    val sc = SparkContextUtil.getInstance()
    if (filePath.trim != "") {
      val file = sc.textFile(filePath)
      val producer = new KafkaProducerUtil()
      logger.info("********  File Lines:  " + file.count() + "  ********")
      var i = 1L
      file.foreach(str => {
        val itmData = new util.HashMap[String, String]()
        itmData.put(itmFile, props.getProperty("data.itm.filename"))
        itmData.put(itmBody, str)
        val jsonData = objectMapper.writeValueAsString(itmData)
        if (i % tNums == 0) {
          logger.info("********  " + DateUtil.getCurrentTimeWithFormat + "  Have To Send JSON Message... " + i + " lines  ********")
          Thread.sleep(tInterval)
        }
        producer.kafkaSend(jsonData)
        i += 1
      })

    } else {
      logger.info("*******  当文件路径为空，则通过目录获取文件  *******")
      val file = sc.wholeTextFiles(fileDir)
      val fileDelimiter = props.getProperty("file.delimiter")
      val producer = new KafkaProducerUtil()
      var i = 1L
      file.collect().length
      val file2 = file.map { case (key, value) => (key.substring(key.lastIndexOf("/") + 1), value.split(fileDelimiter)) }

      file2.foreach { case (key, value) => {
        for (str <- value) {
          val itmData = new util.HashMap[String, String]()
          itmData.put(itmFile, key + "_000001.csv")
          itmData.put(itmBody, str)
          val jsonData = objectMapper.writeValueAsString(itmData)
          if (i % tNums == 0) {
            logger.info("********  " + DateUtil.getCurrentTimeWithFormat + "  Have To Send JSON Message... " + i + " lines  ********")
            Thread.sleep(tInterval)
          }
          producer.kafkaSend(jsonData)
          i += 1
        }
      }
      }

    }
    sc.stop()
  }
}
