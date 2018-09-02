package com.wisdom.spark.streaming.test

import java.util

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.ml.mlUtil.{ContextUtil, ModelUtil}
import com.wisdom.spark.streaming.service.PredResultService
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj
import com.wisdom.spark.streaming.tools.JsonUtil
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhengz on 2017/1/10.
  */
object TestSocketDtata extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val props = ItoaPropertyUtil.getProperties()
    val modelObj = Thread4InitialModelObj.getModelMap()
    println("------------------ props ------------:" + props.hashCode())
    val sc = SparkContextUtil.getInstance()
    println("------------------ sc --------------:" + sc.hashCode())
    println("------------------ props ---------------:" + props.hashCode())
    println("------------------ modelObj ---------------:" + modelObj.hashCode())
    //初始化相关对象（LoadModel、Properties、Bean）
    val initial = new InitUtil(props, modelObj)
    println("------------------ initial ---------------:" + initial.hashCode())
    //设置为广播变量
    val initialBroadCast = sc.broadcast(initial)


    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 6666)
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
          val res = RealTimeDataProcessingNew.dataProcessing(initialBroadCast.value, bodyMap(itmFile).toString, bodyMap(itmBody).toString,"relationAnalysis")
          val perRddEnd = System.currentTimeMillis()
          logger.warn("---------- 单个RDD处理耗时dataProcessing -------------：" + (perRddEnd - perRddSt) + "ms")
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
