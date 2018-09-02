package com.wisdom.spark.streaming.bean

import scala.beans.BeanProperty

/**
  * Created by zhengz on 2017/1/18.
  * 模型对象bean，抽象化需要实例化的模型对象
  */
class ModelObject {
  @BeanProperty
  var modelId: Integer = null
  @BeanProperty
  var modelObjKey: String = ""
  @BeanProperty
  var predIndexName: String = ""
  @BeanProperty
  var predPeriod: String = ""
  @BeanProperty
  var hostName: String = ""
  @BeanProperty
  var preCol: String = ""
  @BeanProperty
  var isValid: String = "N"

  override def toString = s"ModelObject(modelId=$modelId, modelObjKey=$modelObjKey, predIndexName=$predIndexName, predPeriod=$predPeriod, hostName=$hostName, preCol=$preCol, isValid=$isValid)"
}
