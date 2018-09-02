package com.wisdom.spark.streaming.test

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.wisdom.spark.common.ExecutorState
import com.wisdom.spark.common.bean.PredResult
import com.wisdom.spark.common.util.{ItoaPropertyUtil, SysConst}
import com.wisdom.spark.etl.util.RandomNum
import com.wisdom.spark.streaming.service.PredResultService

/**
  * Created by htgeng on 2017/6/1.
  */
object TestPredList {
  def main(args: Array[String]) {
    val props = ItoaPropertyUtil.getProperties()
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val interval = props.getProperty("spark.streaming.interval.second")
    val startTime = "20170611000100"
    var auto = 0

    val cal = Calendar.getInstance()
    val dataTimeTemp = sdf.parse(startTime)
    cal.setTime(dataTimeTemp)

    while (true) {
      Thread.sleep(interval.toLong * 10)
      var batchList: List[PredResult] = List()
      val predResult = new PredResult
      val cpuInt = RandomNum.getRangeNum(1, 20)
      val cpuDec = RandomNum.getRangeNum(1, 99)
      val predMax = RandomNum.getRangeNum(10, 30)
      val predMin = RandomNum.getRangeNum(1, 10)
      val predValue = RandomNum.getRangeNum(1, 40)
      predResult.setCurrentActualValue(cpuInt + "." + cpuDec)
      cal.add(Calendar.MINUTE,5)
      val dateTime=cal.getTimeInMillis


//      val dateTime = dataTimeTemp.getTime + auto * 300 * 1000
//      val startTimeDate = new Date(dateTime)
//      startTime = sdf.format(startTimeDate)
      auto = auto + 1
      val dateTimeFinal = (dateTime / 1000).toString
      predResult.setCurrentDataTime(dateTimeFinal)
      predResult.setCurrentTime(dateTimeFinal)
      predResult.setDataAcceptTime(dateTimeFinal)
      predResult.setExecutorState(ExecutorState.success)
      predResult.setHostName("ASGAAC01")
      predResult.setIndexTyp("00")
      predResult.setModelType("WINSTATS")
      predResult.setNextPredMaxValue(predMax + "")
      predResult.setNextPredMinValue(predMin + "")
      predResult.setNextPredValue(predValue + "")
      predResult.setPredIndexName("IDLE_CPU")
      predResult.setPredPeriod("00")
      predResult.setSysName("SYSTEM")

      batchList :+= predResult
      new PredResultService().testSaveList(batchList)
    }

  }

}
