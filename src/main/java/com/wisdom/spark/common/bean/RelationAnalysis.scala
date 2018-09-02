package com.wisdom.spark.common.bean

import com.wisdom.spark.common.ExecutorState
import com.wisdom.spark.common.ExecutorState._

import scala.beans.BeanProperty

/**
  * Created by wisdom on 2016/12/28.
  */
class RelationAnalysis extends Serializable {
  @BeanProperty
  var relationId: Integer = null
  @BeanProperty
  var sysName: String = ""
  @BeanProperty
  var hostName: String = ""
  @BeanProperty
  var predIndexName: String = ""
  @BeanProperty
  var indexTyp: String = "00"
  @BeanProperty
  var predPeriod: String = "00"
  @BeanProperty
  var currentDataTime: String = ""
  @BeanProperty
  var analysisResult: String = ""
  @BeanProperty
  var executorState: Value = ExecutorState.fail

  override def toString = s"RelationAnalysis(relationId=$relationId, sysName=$sysName, hostName=$hostName, predIndexName=$predIndexName, indexTyp=$indexTyp, predPeriod=$predPeriod, currentDataTime=$currentDataTime, analysisResult=$analysisResult, executorState=${executorState.id}:$executorState)"
}
