package com.wisdom.spark.streaming.thread

import java.util

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.streaming.service.AlarmService
import org.apache.log4j.Logger

/**
  * Created by wisdom on 2016/12/22.
  * 单独封装一个判断告警的class类，如果需要针对某个预测结果进行告警判断则调用该class的run方法（目前已经被合并，暂时淘汰）
  */
@Deprecated
class Thread4CheckAlarm(res: List[util.HashMap[String, String]]) extends Serializable {
  val logger = Logger.getLogger(this.getClass)

  def run(): Unit = {
    logger.info("********************** 判断是否告警 **************************")
    if (res != null && res.size != 0) {
      for (map <- res) {
        val result = map.get("result")
        val alarmService = new AlarmService
//        alarmService.checkIfAlarm(result)
      }
    }
  }
}

//class Thread4CheckAlarm(res: List[util.HashMap[String, String]]) extends Runnable {
//  override def run(): Unit = {
//    println("********************** 判断是否告警 **************************")
//    if (res != null && res.size != 0) {
//      for (map <- res) {
//        val result = map.get("result")
//        val alarmService = new AlarmService
//        alarmService.checkIfAlarm(result)
//      }
//    }
//  }
//}
