package com.wisdom.spark.streaming.test

import java.util

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.ml.mlUtil.ModelUtil
import com.wisdom.spark.streaming.service.PredResultService
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj
import com.wisdom.spark.streaming.tools.JsonUtil
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhengz on 2017/1/10.
  */
object TestSocketDtata2 extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val props = ItoaPropertyUtil.getProperties()
    val sc = SparkContextUtil.getInstance()
    val modelObj = Thread4InitialModelObj.getModelMap()
    val initial = new InitUtil(props, modelObj)
    println("********* modelObj.size\t" + modelObj.size)
    println("------------------ props ------------:" + props.hashCode())
    println("------------------ sc --------------:" + sc.hashCode())
    println("------------------ props ---------------:" + props.hashCode())
    println("------------------ modelObj ---------------:" + modelObj.hashCode())
    //初始化相关对象（LoadModel、Properties、Bean）
    println("------------------ initial ---------------:" + initial.hashCode())
    //设置为广播变量
    val initialBroadCast = sc.broadcast(initial)


    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 6666)
    var dataGetTime = -1L
    val disLines = lines.transform(trans => {
      dataGetTime = System.currentTimeMillis() / 1000 //时间格式还未定制，暂时默认
      trans.distinct()
      val partitions = props.getProperty("kafka.data.partitions").toInt
      trans.repartition(partitions)
    })
    val finalLines = disLines.map(value => {
      JsonUtil.parseJSON(value)
    })
    val itmFile = props.getProperty("data.itm.key")
    val itmBody = props.getProperty("data.itm.value")
    logger.warn("************  RDD map Start...  ************")
    val perStreamSt = System.currentTimeMillis()
    logger.warn("------------  RDD.COUNT()  ------------" + finalLines.count())
    val resultList = finalLines.map(bodyMap => {
      val resObject = new util.HashMap[String, Object]()
      if (bodyMap.contains(itmFile) && bodyMap.contains(itmBody)) {
        resObject.put(SysConst.MAP_DATA_GETTIME_KEY, dataGetTime.toString)
        val perRddSt = System.currentTimeMillis()
        val res = RealTimeDataProcessingNew.dataProcessing(initialBroadCast.value, bodyMap(itmFile).toString, bodyMap(itmBody).toString, "prediction")
        logger.warn("===== res ==>" + res)
        val perRddEnd = System.currentTimeMillis()
        logger.warn("---------- 单个RDD map 处理耗时dataProcessing -------------：" + (perRddEnd - perRddSt) + "ms")
        resObject.put(SysConst.MAP_DATA_RESULT_KEY, res)
        //海涛返回的结果（预测结果和关联分析结果）
      }
      logger.warn("---------- resObject -------------：" + resObject)
      resObject
    })
    val perStreanEnd = System.currentTimeMillis()
    logger.warn("************  RDD map End...  ************")
    logger.warn("------------  当前stream map 处理耗时finalLines.map  ------------：" + (perStreanEnd - perStreamSt) + "ms")
    resultList.foreachRDD(rdd => {
      logger.warn("************  resultList.foreachRDD start...  ************")
      val rddCollectStart = System.currentTimeMillis()
      val list = rdd.collect().toList
      logger.warn("===== list ==>" + list)
      val rddCollectEnd = System.currentTimeMillis()
      logger.warn("------------  当前stream collect 处理耗时 rdd.collect().toList  ------------：" + (rddCollectEnd - rddCollectStart) + "ms")
      logger.warn("************  Save Start...  ************")
      new PredResultService().dataAcceptAndSaveMysql(list)
      val endSave = System.currentTimeMillis()
      logger.warn("************  Save End...  ************")
      logger.warn("------------  当前stream数据保存处理耗时dataAcceptAndSaveMysql  ------------：" + (endSave - rddCollectEnd) + "ms")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
