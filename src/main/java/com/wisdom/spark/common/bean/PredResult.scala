package com.wisdom.spark.common.bean

import com.wisdom.spark.common.ExecutorState
import com.wisdom.spark.common.ExecutorState._

import scala.beans.BeanProperty

/**
  * Created by zhengz on 2016/12/19.
  */
class PredResult extends Serializable {
  @BeanProperty
  var predId: Integer = null
  @BeanProperty
  var sysName: String = ""
  @BeanProperty
  var hostName: String = ""
  @BeanProperty
  var predIndexName: String = ""
  @BeanProperty
  var currentTime: String = ""
  @BeanProperty
  var indexTyp: String = "00"
  @BeanProperty
  var predPeriod: String = "00"
  @BeanProperty
  var dataAcceptTime: String = ""
  @BeanProperty
  var currentDataTime: String = ""
  @BeanProperty
  var currentActualValue: String = ""
  @BeanProperty
  var nextPredValue: String = ""
  @BeanProperty
  var nextPredMinValue: String = ""
  @BeanProperty
  var nextPredMaxValue: String = ""
  @BeanProperty
  var executorState: Value = ExecutorState.fail
  @BeanProperty
  var modelType: String = "PPN"


  override def toString = s"PredResult(predId=$predId, sysName=$sysName, hostName=$hostName, predIndexName=$predIndexName, currentTime=$currentTime, indexTyp=$indexTyp, predPeriod=$predPeriod, dataAcceptTime=$dataAcceptTime, currentDataTime=$currentDataTime, currentActualValue=$currentActualValue, nextPredValue=$nextPredValue, nextPredMinValue=$nextPredMinValue, nextPredMaxValue=$nextPredMaxValue, executorState=${executorState.id}:$executorState, modelType=$modelType)"
}
