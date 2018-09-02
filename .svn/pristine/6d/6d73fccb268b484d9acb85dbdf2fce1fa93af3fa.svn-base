package com.wisdom.spark.etl.util

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.etl.DataProcessing.OfflineDataProcessing
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wisdom on 2016/11/10.
  */
object SeparateByHostname {
  def main(args: Array[String]) : Unit ={
    val properties = ItoaPropertyUtil.getProperties()
    val map = new ConcurrentHashMap[String, AllPredcictTarget] {}
    val init = new InitUtil(properties, map)
    val sc = SparkContextUtil.getInstance()

    val inputPath=init.commonInputPath
    val startTime=init.trainFileStartTime
    val fs=FSUtil.fs
    val filesPath=getSpecFiles(fs,inputPath,startTime)
    println("*************************filesPath:"+filesPath)

    val files = sc.textFile(filesPath)
    val lines = files
      .map(x => (init.dataFormat(x)))//统一各系统数据的时间戳格式
      .map(line => (line.split(",")(init.currentBean.getHostNameInfo).split(":")(init.currentBean.getHostNameIndex), line)) //获取主机名信息
      .filter { case (x, y) => (init.targetMap.contains(init.currentTableName + "_" + x)) } //过滤主机
      .map { case (k, v) => (k, init.getNeededFields(v, init.currentBean.getFieldList, ",")) } //去除无用字段，只获取neededFields字段
//      .map { case (k, v) => (k, init.convertToNumeric(v)) }//转换为非数字，谨慎使用，因为会判断每一个字符
      .map{case (k, v) => (k, init.NaNToNumeric(v)) }//转换为非数字为0
      .filter{case (k, v) => init.afterPeriod(v)}//获取trainFileStartTime之后的数据，提升效率
      .reduceByKey{case (x, y) =>
        x + "\n" + y
      }
    lines.foreach { case (filename, content) => OfflineDataProcessing.offlineDataProcessing(init, filename.toUpperCase(), content) }
    SparkContextUtil.getInstance().stop()
    System.exit(0)
  }


  def getSpecFiles(fs:FileSystem,inputPath:String,startTime:Long):String={
    val sdf=new SimpleDateFormat("yyyyMMddHHmmss")
    val inputFiles = HDFSFileUtil.listFiles(fs, new Path(inputPath))

    val list=new util.ArrayList[String]()
    for (k <- 0 until inputFiles.length) {

      val inputFilePath = inputFiles(k)
      if (fs.isFile(inputFilePath) && !inputFilePath.getName.contains("tmp")&& !inputFilePath.getName.contains("_SUCC")) {
        val fileStatus=fs.getFileStatus(inputFilePath)
        val fileTime=fileStatus.getModificationTime
        val fileTimeDate=new Date(fileTime)
        val fileTimeStr=sdf.format(fileTimeDate)
        if(fileTimeStr.toLong>startTime){
          list.add(inputFilePath.toString)
        }
      }
    }
    list.toArray.mkString(",")
  }
}
