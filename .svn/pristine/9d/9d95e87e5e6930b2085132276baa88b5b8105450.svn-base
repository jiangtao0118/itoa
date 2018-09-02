package com.wisdom.spark.streaming.thread

import java.util
import java.util.Properties

import com.wisdom.spark.common.bean.{PredResult, RelationAnalysis}
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.ml.mlUtil.{ContextUtil, ModelUtil}
import com.wisdom.spark.streaming.service.{DataService, PredResultService}
import com.wisdom.spark.streaming.tools.{JsonUtil, ParamUtil}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhengz on 2016/12/12.
  * 该线程类用于最新版streaming接收kafka消息并解析进行预测的main函数入口，启动main函数监听kafka消息
  * 具体操作包括：
  * streaming接收kafka消息转换为RDD;
  * 将RDD进行简单去重复操作和JSON解析操作;
  * 遍历RDD进行模型预测和关联分析操作;
  * 将预测结果和关联分析结果进一步封装调用业务层逻辑批量保存至数据库
  */
@deprecated
object Thread4ITMDataCollect2 extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)


  /**
    * 1.Kerb认证
    * 2.创建sparkConf:SparkConf
    * 3.创建ssc:SparkStreamingContext
    * 4.创建KafkaParms
    * 5.收集ITM消息
    * 6.去重复并解析消息
    * 7.调用etl接口
    * 8.返回结果保存MySQLS
    *
    * @param args
    */
  def main(args: Array[String]) {
    //    val sparkConf = new SparkConf().setAppName(appName)
    //******** modify by zhengz at 20170106 *********
    //    val props = ContextUtil.props
    val props = ItoaPropertyUtil.getProperties()
    val modelObj = Thread4InitialModelObj.getModelMap()
    println("------------------ props ------------:" + props.hashCode())
    val appName = props.getProperty("public.app.name")
    val streamPeriod = props.getProperty("spark.streaming.interval.second")
    val checkPoints = props.getProperty("spark.streaming.checkpoint")
    val itmTopics = props.getProperty("kafka.topic.itm.topic")
    val brokers = props.getProperty("kafka.common.brokers")
    val offset = props.getProperty("kafka.param.offset.value")

    //    val sparkContext = ContextUtil.sc
    val sparkContext = SparkContextUtil.getInstance()
    println("------------------ sc --------------:" + sparkContext.hashCode())
    sparkContext.getConf.setAppName(appName)
    println("------------------ props ---------------:" + props.hashCode())
    println("------------------ modelObj ---------------:" + modelObj.hashCode())
    //初始化相关对象（LoadModel、Properties、Bean）
    val initial = new InitUtil(props, modelObj)
    println("------------------ initial ---------------:" + initial.hashCode())
    //设置为广播变量
    val initialBroadCast = sparkContext.broadcast(initial)

    val ssc = new StreamingContext(sparkContext, Seconds(streamPeriod.toInt))
    ssc.checkpoint(checkPoints)
    val topicsSet = itmTopics.split(",").toSet
    val kafkaParams = Map[String, String](props.getProperty("kafka.param.brokers.key") -> brokers, props.getProperty("kafka.param.offset.key") -> offset)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    var dataGetTime = -1L
    val disLines = lines.transform(trans => {
      dataGetTime = System.currentTimeMillis() / 1000 //时间格式还未定制，暂时默认
      trans.distinct()
    })
    val finalLines = disLines.map(value => {
      JsonUtil.parseJSON(value)
    })
    finalLines.foreachRDD(rdd => {
      logger.warn("************  RDD Start...  ************")
      val perStreamSt = System.currentTimeMillis()
      logger.warn("------------  RDD.COUNT()  ------------" + rdd.count())
      val itmFile = props.getProperty("data.itm.key")
      val itmBody = props.getProperty("data.itm.value")
      val resultList = rdd.map(bodyMap => {
        val resObject = new util.HashMap[String, Object]()
        if (bodyMap.contains(itmFile) && bodyMap.contains(itmBody)) {
          resObject.put(SysConst.MAP_DATA_GETTIME_KEY, dataGetTime.toString)
          val perRddSt = System.currentTimeMillis()
          val res = RealTimeDataProcessingNew.dataProcessing(initialBroadCast.value, bodyMap(itmFile).toString, bodyMap(itmBody).toString,"prediction")
          val perRddEnd = System.currentTimeMillis()
          logger.warn("------------  单个RDD处理耗时dataProcessing  ------------：" + (perRddEnd - perRddSt) + "ms")
          resObject.put(SysConst.MAP_DATA_RESULT_KEY, res)
          //海涛返回的结果（预测结果和关联分析结果）
        }
        logger.warn("---------- resObject -------------：" + resObject)
        resObject
      }).collect().toList
      val perStreanEnd = System.currentTimeMillis()
      logger.warn("************  RDD End...  ************")
      logger.warn("------------  单个stream处理耗时finalLines.foreachRDD  ------------：" + (perStreanEnd - perStreamSt) + "ms")
      logger.warn("************  Save Start...  ************")
      new PredResultService().dataAcceptAndSaveMysql(resultList)
      val endSave = System.currentTimeMillis()
      logger.warn("************  Save End...  ************")
      logger.warn("------------  stream数据保存处理耗时dataAcceptAndSaveMysql  ------------：" + (endSave - perStreanEnd) + "ms")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

