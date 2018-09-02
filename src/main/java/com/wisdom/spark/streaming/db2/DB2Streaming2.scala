package com.wisdom.spark.streaming.db2

import java.util.Properties

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.ml.tgtVar.DB2Diag
import com.wisdom.spark.streaming.service.AlarmService
import com.wisdom.spark.streaming.tools.JsonUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by wisdom on 2017/7/4.
  */
object DB2Streaming2 {
  @transient
  //用于写日志的实例
  val logger = Logger.getLogger(this.getClass)
  //获取配置文件
  //val props = ItoaPropertyUtil.getProperties()

  def main(args: Array[String]) {
    //公共方法：获取SparkContext
   // val sc = SparkContextUtil.getInstance()
    val props = ItoaPropertyUtil.getProperties()
    val appName = props.getProperty("public.app.name")
    val sparkConf = new SparkConf().setAppName(appName)
    val master = props.getProperty("sparkMaster")
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      System.setProperty("hadoop.home.dir", "c:/winutils")
      sparkConf.setMaster(master)
    }
    //val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(sparkConf)
//    db2diagalarm(sc,props)
//    //获取SparkStreaming的监听时间间隔
//  }
//  def db2diagalarm(sc:SparkContext,props:Properties)={
    val streamPeriod = props.getProperty("db2.streaming.interval.second")
    //Spark方法：StreamingContext（sc,时间（秒））
    val ssc = new StreamingContext(sc, Seconds(streamPeriod.toLong))
    //获取设置检查点的路径
    val checkPoints = props.getProperty("db2.streaming.checkpoint")
    //设置检查点
    ssc.checkpoint(checkPoints)
    //kafka的broker列表
    val brokers = props.getProperty("kafka.common.brokers")
    //Kafka 配置参数
    // Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s) (NOT zookeeper servers),
    // specified in host1:port1,host2:port2 form.
    val kafkaParams = Map[String, String](props.getProperty("kafka.param.brokers.key") -> brokers)
    //Kafka Topic
    val db2diagTopics = props.getProperty("kafka.topic.db2diag")
    //Kafka Topic to Set
    val setDB2 = db2diagTopics.split(",").toSet
    //获取到的数据中，节点的键名
    val db2HostName = props.getProperty("data.db2.key")
    //获取到的数据中，目标数据的键名
    val db2Message = props.getProperty("data.db2.value")
    //重分区的分区数量
    val partitions = props.getProperty("kafka.data.partitions").toInt
    //低频词阈值，小于此阈值的都认为是低频日志
    val threshold = props.getProperty("data.db2.threshold").toInt

    //加载模型
    val hostNameArr = props.getProperty("data.db2.hostname").split(",")
    val modelMap = scala.collection.mutable.Map[String,DB2Diag]()
    for (hostName <- hostNameArr){
      val db2diag = new DB2Diag("DB2DIAG", hostName)
      modelMap += (hostName -> db2diag)
    }
    logger.warn("~~~~~~~~~~~~~~~~~~~modelMap:"+modelMap.toString()+"~~~~~~~~~~~~~~")
    //创建Kafka-Streaming 返回DStream
    val lineDB2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, setDB2).map(_._2)
    // 数据去重、重分区、排序
    val finalLineDB2 = lineDB2.transform(trans => {
      //去重、重分区
      trans.distinct().repartition(partitions)
      //JSON解析成Map(key->value)
    }).map(JsonUtil.parseJSON(_)).filter(map => {
      //数据过滤，条件：包含键@filename,@message,且@message有内容
      //if (map.contains(db2HostName) && map.contains(db2Message) && !map(db2Message).toString.isEmpty) true else false
      //获取@message的内容
      map.contains(db2HostName) && map.contains(db2Message) && !map(db2Message).toString.isEmpty
    }).map(map => (map(db2HostName).toString, map(db2Message).toString)).filter(
      values => modelMap.contains(values._1)
    ).map(values => {
      //创建DB2Diag实例
      //  val db2diag = modelMap(values._1.toString)
      //调用lowfreq方法，返回DB2diagOut对象
      //  db2diag.lowfreq(values._2.toString, threshold)
      val db2diag = new DB2Diag("DB2DIAG",values._1.toString).lowfreq(values._2.toString,threshold)
      db2diag
    })
    //DStream保存
    finalLineDB2.foreachRDD(rdd => {
      //将预测结果转化为list
      val list = rdd.repartition(partitions).collect().toList
      //将预测结果保存到MySQL数据库中
      new AlarmService().checkDB2LogIfAlarm(list)
    })
    //    启动流计算环境StreamingContext并等待它“完成”
    ssc.start()
    //    等待作业完成
    ssc.awaitTermination()
  }

}
