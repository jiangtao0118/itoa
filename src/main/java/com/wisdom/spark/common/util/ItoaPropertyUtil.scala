package com.wisdom.spark.common.util

import java.io.{FileInputStream, SequenceInputStream}
import java.util.{Properties, Vector}

/**
  * Created by zhengz on 2017/1/11.
  *
  * 整合配置文件获取方式，作为工具类，单例模式统一调用
  */
object ItoaPropertyUtil extends Serializable{
  var mlPath = "conf/ml_properties.properties"
  var etlPath = "conf/etl_properties.properties"
  var streamPath = "conf/streaming_properties.properties"
  val homeDir = ""

  var properties: Properties = null

  /**
    * 判断properties是否为空，判断是操作系统，分别读取配置文件，配置文件合并流处理，Properties.load
    *
    * @return 属性配置文件对象（key,value）
    */
  def getProperties(): Properties = {
    try {
      if (properties == null || properties.isEmpty) {
        properties = new Properties()
        var sis: SequenceInputStream = null
        if (!System.getProperty("os.name").contains("Windows")) {
          mlPath = homeDir + mlPath
          etlPath = homeDir + etlPath
          streamPath = homeDir + streamPath
        }
        val input1 = new FileInputStream(mlPath)
        val input2 = new FileInputStream(etlPath)
        val input3 = new FileInputStream(streamPath)
        val streams: Vector[FileInputStream] = new Vector[FileInputStream]()
        streams.add(input1)
        streams.add(input2)
        streams.add(input3)
        sis = new SequenceInputStream(streams.elements())
        properties.load(sis)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    properties
  }


  /**
    * 判断properties是否为空，判断是操作系统，分别读取配置文件，配置文件合并流处理，Properties.load
    * @param rootPath 配置文件根路径，如/home/wsd/zhengz/
    * @return 属性配置文件对象（key,value）
    */
  def getProperties(rootPath:String): Properties = {
    try {
      if (properties == null || properties.isEmpty) {
        properties = new Properties()
        var sis: SequenceInputStream = null
        if (!System.getProperty("os.name").contains("Windows")) {
          mlPath = rootPath + mlPath
          etlPath = rootPath + etlPath
          streamPath = rootPath + streamPath
        }
        val input1 = new FileInputStream(mlPath)
        val input2 = new FileInputStream(etlPath)
        val input3 = new FileInputStream(streamPath)
        val streams: Vector[FileInputStream] = new Vector[FileInputStream]()
        streams.add(input1)
        streams.add(input2)
        streams.add(input3)
        sis = new SequenceInputStream(streams.elements())
        properties.load(sis)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    properties
  }

}
