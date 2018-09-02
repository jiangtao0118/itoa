package com.wisdom.spark.streaming.test

import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.ml.mlUtil.{ContextUtil, HDFSFileUtil, ModelUtil}
import com.wisdom.spark.streaming.service.ModelObjService
import org.apache.hive.common.util.HDFSUtils

/**
  * Created by zhengz on 2017/1/18.
  */
object TestModelObjService {
  def main(args: Array[String]) {
    //    val rootPath = ContextUtil.mlRootPath
    //    println("rootPath:" + rootPath)
    //    val arrFile = HDFSFileUtil.listFiles(rootPath)
    //    val arrFile2 = arrFile.filter(_.matches("\\w*_5$|\\w*_15$|\\w*_30$|\\w*_1H$"))
    //    println("----------: 指标加周期 ")
    //    println("----------: " + arrFile2.mkString(","))
    //    for (indexName <- arrFile2) {
    //      val arrHosts = HDFSFileUtil.listFiles(rootPath + "\\" + indexName)
    //      println(indexName + "指标下的主机：")
    //      println(arrHosts.mkString(","))
    //      println("--------- 拆分：")
    //      val n1 = indexName.substring(0, indexName.lastIndexOf("_"))
    //      val n2 = indexName.substring(indexName.lastIndexOf("_") + 1)
    //      println(n1 + "******" + n2)
    //    }
    //    val se = new ModelObjService()
    //    se.initialModelObj()
    //    val s = se.findModelObj()
    //    for (i <- 0 until (s.size())) {
    //      println(s.get(i).toString)
    //    }
    //    println(s.size() + "---" + s)
    println(ModelUtil.modelMap)
    //    println(ModelUtil.modelMapDB)
  }


}
