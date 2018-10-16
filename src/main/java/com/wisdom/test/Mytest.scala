package com.wisdom.test


import breeze.linalg.*
import com.wisdom.spark.common.util.SparkContextUtil
import org.apache.commons.csv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.immutable.HashMap


object Mytest {

  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
    val df = spark.read.option("header","true").csv("data.csv")


  }

  def echo( str:String*):Unit={
    for(arg <- str){
      print(arg)
    }
  }

}
