package com.wisdom.spark.streaming.bean

import scala.beans.BeanProperty

/**
  * Created by htgeng on 2017/6/29.
  */
class DataBean {
  @BeanProperty
  var transData=""
  @BeanProperty
  var systemData=""
  @BeanProperty
  var unix_memoryData=""

}
