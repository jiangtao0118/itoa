package com.wisdom.spark.etl.bean

import java.util

import scala.beans.BeanProperty

/**
  * Created by htgeng on 2017/1/12.
  */
class SystemBean extends Serializable{
  //分隔符
  @BeanProperty
  var delimiter=","
  //存储neededFields在All_fields中的索引信息
  @BeanProperty
  var fieldList=new util.ArrayList[Int]
  //主机名在All_fields中的索引信息
  @BeanProperty
  var hostNameIndex=0
  @BeanProperty
  var hostNameInfo=0
  @BeanProperty
  var neededFields=new Array[String](0)
  @BeanProperty
  var predictionField=""
  @BeanProperty
  var predictionFields=""
  @BeanProperty
  var predictionFieldsMap=new util.HashMap[String,Int]()
  @BeanProperty
  var predictionFieldIndex=0
  //预测字段范围
  @BeanProperty
  var predictionFieldFilterRule="10,100"
  @BeanProperty
  var systemName=""
  @BeanProperty
  var timestampIndex=""
}
