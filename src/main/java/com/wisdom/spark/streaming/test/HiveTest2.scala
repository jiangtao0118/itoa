package com.wisdom.spark.streaming.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.wisdom.spark.common.util.SparkContextUtil
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/6/12.
  */
object HiveTest2 {

  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
  //  val sc = SparkContextUtil.getInstance()
    val conf = new SparkConf().setMaster("spark://local:7077").setAppName("My App")
    val sc = new SparkContext(conf)
    //    新建HiveContext对象
    val hiveCtx = new HiveContext(sc)
    UpdateHiveInput(hiveCtx)

  }

  def UpdateHiveInput(hiveContext: HiveContext) : Unit = {
    val table1 = hiveContext.sql("desc default.employee")

    println("---------------------------------------------")
    println(table1.count())
    table1.show(10)
    println("---------------------------------------------"+table1.count())
  }

}
