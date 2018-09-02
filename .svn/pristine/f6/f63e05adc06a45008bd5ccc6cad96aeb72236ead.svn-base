package com.wisdom.spark.ml.mlUtil

import java.util.Properties

import com.wisdom.java.common.LoginUtil
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.streaming.tools.ParamUtil
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by wisdom on 2016/11/12.
  */
object ContextUtil {
  val props=ItoaPropertyUtil.getProperties()
  val sc = SparkContextUtil.getInstance()
  val sqlContext = new SQLContext(sc)
  val hdfsConf = sc.hadoopConfiguration
  //.addResource(path + "hdfs-site.xml").addResource(path + "mapred-site.xml")

  val mlLogPropsPath = PropertyUtil.getProperty("mlLogProps")
  val mlRootPath = PropertyUtil.getProperty("mlRootPath")
  val mlTrainCount = PropertyUtil.getProperty("mlTrainWeeks").toInt*2016
  val mlWindow=PropertyUtil.getProperty("mlWindow").toInt
//  val hiveCtx=new HiveContext(sc)

  @transient
  val logger = LogUtil.getModelLogger

  val BOUND_MAP = scala.collection.mutable.Map[String, Array[Double]]()
  val tgts = PropertyUtil.getProperty("ml.tgts").split(",").map(x=>x.trim)
  for (i <- 0 until tgts.length) {
    var value=PropertyUtil.getProperty("ml.bound_map." + tgts(i))
    if(value==null){
      println("指标" + tgts(i) + "未设定上下界，将下界、上界分别设置为默认值0、1E10")
      value="0,1E10"
    }
    BOUND_MAP.put(tgts(i), value.split(",").map(x => x.toDouble))
  }

  val INTERVAL = PropertyUtil.getProperty("ml.interval")

  val winstatstgt=PropertyUtil.getProperty("ml.winstats.tgts").split(",")

  val winstatsFreq5min=PropertyUtil.getProperty("ml.winstats.freq.5min").split(",")

  val model_ppn=PropertyUtil.getProperty("ml.model_type.PPN")
  val model_winstats=PropertyUtil.getProperty("ml.model_type.WINSTATS")
  val model_tfidf=PropertyUtil.getProperty("ml.model_type.TFIDF")

//  val indexInfoMap=new mutable.HashMap[String,String]()
//  for(target<-tgts){
//    if(props.containsKey("ml.model.index."+target)){
//      val indexInfo=props.getProperty("ml.model.index."+target)
//      indexInfoMap.put(target,indexInfo)
//    }
//  }



}
