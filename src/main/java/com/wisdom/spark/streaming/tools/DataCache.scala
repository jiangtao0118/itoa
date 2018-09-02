package com.wisdom.spark.streaming.tools

import java.util

import scala.collection.mutable.ArrayBuffer

/**
  * 创建一个object类用于缓存driver端相关集合信息
  * Created by zhengz on 2017/3/14.
  */
object DataCache {

  /*模型预测记录前面N条预测记录的数据时间*/
  val predictTimes = new util.HashMap[String, ArrayBuffer[String]]()


}
