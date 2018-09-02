package com.wisdom.spark.streaming.tools

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  * Created by zhengz on 2016/11/16.
  * 配置文件读取工具类
  */
object ParamUtil {


  /**
    * 读取classpath目录下的配置文件
    *
    * @return
    */
//  def getProperties(): Properties = {
//    var fileName = "/home/wsd/zhengz/conf/streaming_properties.properties"
//
//    if (System.getProperty("os.name").contains("Windows")) {
//      fileName = "./conf/streaming_properties.properties"
//    } else {
//      fileName = "/home/wsd/zhengz/conf/streaming_properties.properties"
//    }
//
//    val prop = new Properties()
//    //    val file = new File(fileName)
//    //    val path = Thread.currentThread().getContextClassLoader.getResource(fileName).getPath //文件要放到resource文件夹下
//    prop.load(new FileInputStream(fileName))
//    return prop
//  }
}
