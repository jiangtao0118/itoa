package com.wisdom.spark.ml.mlUtil

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.wisdom.spark.common.util.ItoaPropertyUtil

/**
  * Created by wisdom on 2016/11/14.
  */
object PropertyUtil {

  def getProperty(key: String): String = {
//    val properties = new Properties()
//    var path = "conf/ml_properties.properties"
//    if (!System.getProperty("os.name").contains("Windows")) {
//      path = "/home/wsd/zhengz/conf/ml_properties.properties"
//    }
//
//    properties.load(new FileInputStream(path))
//    properties.getProperty(key)

    ItoaPropertyUtil.getProperties.getProperty(key)
  }

  /**
    * 修改或添加键值对 如果key存在，修改 反之，添加。
    *
    * @param key
    * @param value
    */
  def saveModelProperty(key: String, value: String): Unit = {

    val properties = new Properties()
    //文件放到resource文件夹下
    val modelProPath = getProperty("modelPropPath")
    val file = new File(modelProPath)
    if (!file.exists()) {
      file.createNewFile()
    }

    val fis = new FileInputStream(file)
    properties.load(fis)
    fis.close() //一定要在修改值之前关闭fis

    val fos = new FileOutputStream(modelProPath)
    properties.setProperty(key, value)
    properties.store(fos, "")
    fos.close()

  }

  def getModelProperty(key: String): String = {
    val properties = new Properties()
    val path = getProperty("modelPropPath")
    properties.load(new FileInputStream(path))
    properties.getProperty(key)
  }
}
