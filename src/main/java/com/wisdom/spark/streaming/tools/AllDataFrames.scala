package com.wisdom.spark.streaming.tools

import java.text.SimpleDateFormat
import java.util.Date

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.ml.mlUtil.{ContextUtil, PropertyUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by htgeng on 2017/7/3.
  */
object AllDataFrames {
//  val props=ItoaPropertyUtil.getProperties()
//  val sc = SparkContextUtil.getInstance()
//  val hiveCtx=new HiveContext(sc)
//
//
//  val sdfDst=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
//  val mlTrainWeeks = PropertyUtil.getProperty("mlTrainWeeks").toInt
//
//  val trainEndTime = new Date()
//  val trainEndTimeLong=trainEndTime.getTime
//  val trainStartTimeLong = trainEndTimeLong -  mlTrainWeeks*7*24*60*60*1000
//
//  val trainStartTime = new Date(trainStartTimeLong)
//
//  val dateStartDst=sdfDst.format(trainStartTime)
//  val dateEndDst=sdfDst.format(trainEndTime)
//
//  val allData=queryHive(hiveCtx,dateStartDst,dateEndDst)
//
//  val transDF=allData._1
//  val sysDF=allData._2
//  val unix1DF=allData._3
//  val unix2DF=allData._4
//  val ncoDF=allData._5
//
//  if(transDF!=null){
//    transDF.registerTempTable("apptrans")
//    transDF.cache()
//  }
//  if(sysDF!=null){
//    sysDF.registerTempTable("system")
//    sysDF.cache()
//  }
//  if(unix1DF!=null){
//    unix1DF.registerTempTable("unix_memory1")
//    unix1DF.cache()
//  }
//  if(unix2DF!=null){
//    unix2DF.registerTempTable("unix_memory2")
//    unix2DF.cache()
//  }
//  if(ncoDF!=null){
//    ncoDF.registerTempTable("reporter_status")
//    ncoDF.cache()
//  }
//
//  def queryHive(hiveCtx:HiveContext,startTime:String,endTime:String):(DataFrame,DataFrame,DataFrame,DataFrame,DataFrame) = {
//    var transDF:DataFrame=null
//    if(props.containsKey("ml.hive.data.trans.ecupin")){
//      val ecupinDF = hiveCtx.sql(props.getProperty("ml.hive.data.trans.ecupin").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//      val ecupoutDF = hiveCtx.sql(props.getProperty("ml.hive.data.trans.ecupout").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//      val gaacDF = hiveCtx.sql(props.getProperty("ml.hive.data.trans.gaac").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//      val mobsDF = hiveCtx.sql(props.getProperty("ml.hive.data.trans.mobs").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//      transDF = ecupinDF.unionAll(ecupoutDF).unionAll(gaacDF).unionAll(mobsDF)
//    }
//
////    val tempTableopm1 = hiveCtx.sql("select opm_min_collection_timestamp as time,appls_cur_cons as value,opm_db_host_name as hostname  from hist_opm.opm_db where opm_min_collection_timestamp >"+startTime +" and opm_min_collection_timestamp < "+endTime)
////    val tempTableopm2 = hiveCtx.sql("select opm_min_collection_timestamp as time,appls_in_db2 as value,opm_db_host_name as hostname  from hist_opm.opm_db where opm_min_collection_timestamp >"+startTime +" and opm_min_collection_timestamp < "+endTime)
////    val opmDF = tempTableopm1.unionAll(tempTableopm2)
//    var systemDF:DataFrame=null
//    if(props.containsKey("ml.hive.data.system")){
//      systemDF= hiveCtx.sql(props.getProperty("ml.hive.data.system").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//    }
//
//    var unixmem1:DataFrame=null
//    var unixmem2:DataFrame=null
//    if(props.containsKey("ml.hive.data.unix_memory1")){
//      unixmem1= hiveCtx.sql(props.getProperty("ml.hive.data.unix_memory1").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//      unixmem2= hiveCtx.sql(props.getProperty("ml.hive.data.unix_memory2").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//    }
//
//    var ncoperfDF:DataFrame=null
//    if(props.containsKey("ml.hive.data.reporter_status")){
//      ncoperfDF = hiveCtx.sql(props.getProperty("ml.hive.data.reporter_status").replace("STARTTIME",startTime).replace("ENDTIME",endTime).trim)
//    }
//
//    (transDF,systemDF,unixmem1,unixmem2,ncoperfDF)
//  }

}
