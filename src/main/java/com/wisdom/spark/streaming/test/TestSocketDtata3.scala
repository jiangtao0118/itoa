package com.wisdom.spark.streaming.test

import java.util

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.streaming.service.PredResultService
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj
import com.wisdom.spark.streaming.tools.JsonUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhengz on 2017/1/10.
  */
object TestSocketDtata3 extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    //获取配置文件
    val props = ItoaPropertyUtil.getProperties()
    //创建SparkContext
    val sparkContext = SparkContextUtil.getInstance()
    //加载用于预测的模型
    val modelObj = Thread4InitialModelObj.getModelMap()
    //打印加载模型的数量
    println("********* modelObj.size\t" + modelObj.size)
    //获取配置的APPName
    val appName = props.getProperty("public.app.name")
    //获取SparkStreaming的监听时间间隔
    val streamPeriod = props.getProperty("spark.streaming.interval.second")
    //获取设置检查点的路径
    val checkPoints = props.getProperty("spark.streaming.checkpoint")
    //获取来自于ITM的数据的Topic名称
    val itmTopics = props.getProperty("kafka.topic.itm")
    //获取来自于OPM的数据的Topic名称
    val opmTopics = props.getProperty("kafka.topic.opm")
    //获取来自于AppTrans的数据的Topic名称
    val appTransTopics = props.getProperty("kafka.topic.apptrans")

    //获取来自于AppTrans的数据的Topic名称
    val ncoperfTopics = props.getProperty("kafka.topic.ncoperf")

    //获取kafka配置的broker
    val brokers = props.getProperty("kafka.common.brokers")

    //设置Spark程序运行时的名字
    sparkContext.getConf.setAppName(appName)
    //调用预测接口时所需的参数
    val initial = new InitUtil(props, modelObj)
    //设置为广播变量
    val initialBroadCast = sparkContext.broadcast(initial)
    //创建StreamingContext
    val ssc = new StreamingContext(sparkContext, Seconds(streamPeriod.toInt))
    //设置检查点
    ssc.checkpoint(checkPoints)
    //将获取到的Topic配置项，用逗号分隔，转化为Set,便于后面创建KafkaDstream时参数的调用
    val setItm = itmTopics.split(",").toSet
    val setOpm = opmTopics.split(",").toSet
    val setTrans = appTransTopics.split(",").toSet
    //Kafka Topic to Set
    val setNco = ncoperfTopics.split(",").toSet
    //接收到的Kafka JSON数据中，目标键值对的键值
    val itmFile = props.getProperty("data.itm.key")
    val itmBody = props.getProperty("data.itm.value")
    //创建kafkaDStream时所需的kafka配置项
    val kafkaParams = Map[String, String](props.getProperty("kafka.param.brokers.key") -> brokers) //, props.getProperty("kafka.param.offset.key") -> offset
    //创建Topic:ITM的KafkaDStream，参见Spark KafkaUtils API
    val lineItm = ssc.textFileStream("/user/wsd/streaming/")
    //创建Topic:OPM的KafkaDStream，参见Spark KafkaUtils API
//    val lineOpm = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, setOpm).map(_._2)
//    //创建Topic:APPTRANS的KafkaDStream，参见Spark KafkaUtils API
//    val lineTrans = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, setTrans).map(_._2)
//    //创建Kafka-Streaming 返回DStream
//    val lineNco = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, setNco).map(_._2)
//    //数据接收时间
    var dataGetTime = -1L
    //RDD重分区分区数
    val partitions = props.getProperty("kafka.data.partitions").toInt
//    val lineItm = ssc.textFileStream("/user/wsd/streaming/")


    // ITM OPM APPTRANS数据处理
    val resultLineITM = lineItm.transform(trans => {
      //获取Kafka数据接收时间，时间格式还未定制，暂时默认
      dataGetTime = System.currentTimeMillis() / 1000
      //去重复，重新分区
      trans.distinct().repartition(partitions)
      //JSON解析成Map
    }).map(JsonUtil.parseJSON(_)).filter(map => {
      //数据过滤,仅保留数据中包含键 @filename 和 @message 的数据
      if (map.contains(itmFile) && map.contains(itmBody)) true else false
      //数据抽取，取@filename和@message的内容，组成元组
    }).map(map => (map(itmBody).toString, map(itmFile).toString)).transform(trans => {
      //数据排序
      trans.sortByKey(true)
    }).map(values => {
      //用于封装预测结果的变量resObject
      val resObject = new util.HashMap[String, Object]()
      //调用预测接口，返回预测结果Map
      val res = RealTimeDataProcessingNew.dataProcessing(initialBroadCast.value, values._2, values._1, "prediction")
      //将预测结果和Kafka数据接收时间封装到一起
      resObject.put(SysConst.MAP_DATA_GETTIME_KEY, dataGetTime.toString)
      resObject.put(SysConst.MAP_DATA_RESULT_KEY, res)
      //返回封装好的预测结果
      resObject
    })

    val finalLine = resultLineITM
    //** 执行action操作,保存预测结果
    finalLine.foreachRDD(rdd => {
      //rdd 转为List的开始时间
      val t1 = System.currentTimeMillis()
      //rdd 转为List
      val list = rdd.collect().toList

      //rdd 转为List的结束时间
      val t2 = System.currentTimeMillis()
      logger.warn("****  当前stream collect 处理耗时 rdd.collect().toList  ===> " + (t2 - t1) + " ms")
      //保存预测结果到MySql中
      new PredResultService().dataAcceptAndSaveMysql(list)
      //预测结果保存结束时间
      val t3 = System.currentTimeMillis()
      logger.warn("****  当前stream数据保存处理耗时dataAcceptAndSaveMysql  ===> " + (t3 - t2) + " ms")
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
