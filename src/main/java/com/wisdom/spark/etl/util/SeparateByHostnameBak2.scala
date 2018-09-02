package com.wisdom.spark.etl.util

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.etl.DataProcessing.OfflineDataProcessing
import com.wisdom.spark.ml.mlUtil.ModelUtil
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj

/**
  * Created by wisdom on 2016/11/10.
  */
@deprecated
object SeparateByHostnameBak2 {

  def main(args: Array[String]) {
    val properties = ItoaPropertyUtil.getProperties()
    val map = new ConcurrentHashMap[String, AllPredcictTarget] {}
    val init = new InitUtil(properties, map)
    val sc = SparkContextUtil.getInstance()
    val files = sc.textFile(init.commonInputPath + "*")
    val lines = files
      .map(line => (init.dataFormat(line)))
      .map(line => (line.split(",")(init.currentBean.getHostNameInfo).split(":")(init.currentBean.getHostNameIndex), line)) //获取主机名信息
      .filter { case (x, y) => (init.targetMap.contains(init.currentTableName + "_" + x)) } //过滤主机
      .map { case (k, v) => (k, init.getNeededFields(v, init.currentBean.getFieldList, ",")) } //remove unused fields and convert timestamp
//      .map { case (k, v) => (k, init.convertToNumeric(v)) }
      .map { case (k, v) => (v, k) }
      .sortByKey()
      .map { case (v, k) => (k, v) }
      .reduceByKey((x, y) => x + "\n" + y)

    val allTimeFile =lines.map { case (filename, contentStr) =>
      val content = contentStr.split("\n")
      val column_current_3 = content.take(content.length - 8)
      val column_current_2 = content.tail.take(column_current_3.length)
      val column_current_1 = content.tail.tail.take(column_current_3.length)
      val column_current = content.tail.tail.tail.take(column_current_3.length)

      val m01=init.minus(column_current,column_current_1)
      val m12=init.minus(column_current_1,column_current_2)
      val m23=init.minus(column_current_2,column_current_3)


      val predictionField = content.map(_.split(",")(init.currentBean.getPredictionFieldIndex))
      val predictionField_p1 = predictionField.tail.tail.tail.tail.take(column_current_3.length)
      val predictionField_p2 = predictionField.tail.tail.tail.tail.take(column_current_3.length)
      val predictionField_p3 = predictionField.tail.tail.tail.tail.take(column_current_3.length)
      val predictionField_p4 = predictionField.tail.tail.tail.tail.take(column_current_3.length)
      (filename, column_current_3, column_current_2, column_current_1, column_current,m01,m12,m23, predictionField_p1, predictionField_p2, predictionField_p3, predictionField_p4)
    }

    val allFile=allTimeFile.map{case (filename,c3,c2,c1,c,m01,m12,m23,p1,p2,p3,p4)=>
      val content=c+","+c1+","+c2+","+c3+","+m01+","+m12+","+m23+","+p1
      (filename,content)
    }

    allFile.saveAsTextFile("/user/wsd/temp1")

    val normalDataFile=allTimeFile.filter{case (filename,c3,c2,c1,c,m01,m12,m23,p1,p2,p3,p4)=>
      Math.abs(p1.toString.toDouble).compareTo(0)>50
    }.map{case (filename,c3,c2,c1,c,m01,m12,m23,p1,p2,p3,p4)=>
      val content=c+","+c1+","+c2+","+c3+","+m01+","+m12+","+m23+","+p1
      (filename,content)
    }

    normalDataFile.saveAsTextFile("/user/wsd/temp2")


    val abnormalDataFile=allTimeFile.filter{case (filename,c3,c2,c1,c,m01,m12,m23,p1,p2,p3,p4)=>
      Math.abs(p1.toString.toDouble).compareTo(0)<50
    }.map{case (filename,c3,c2,c1,c,m01,m12,m23,p1,p2,p3,p4)=>
      val content=c+","+c1+","+c2+","+c3+","+m01+","+m12+","+m23+","+p1
      (filename,content)
    }

    abnormalDataFile.saveAsTextFile("/user/wsd/temp3")

    SparkContextUtil.getInstance().stop()
  }
}
