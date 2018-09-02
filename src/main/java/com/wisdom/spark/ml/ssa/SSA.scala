package com.wisdom.spark.ml.ssa

import java.text.SimpleDateFormat
import java.util
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import com.wisdom.spark.streaming.service.PredResultService
import com.wisdom.spark.streaming.thread.{Thread4GAACPredAna, Thread4InitialModelObj}
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/3/20.
  */
object SSA {
  @transient
  val logger = Logger.getLogger(this.getClass)

  private var HiveCtx :HiveContext = null
  /**
    * 获取HiveContext实例
    *
    * @return HiveContext
    */
  def getHiveCtx():HiveContext={
    if (HiveCtx == null) {
      HiveCtx= new HiveContext(SparkContextUtil.getInstance())
      HiveCtx
    }
    else
      HiveCtx
  }

  /**
    * 时间格式转换，
    * @param date 时间字符串格式 201703211017
    * @return
    */
  def getDate2Seconds(date:String):Long={
    val fm = new SimpleDateFormat("yyyyMMddHHmm")
    val dt2 = fm.parse(date)
    dt2.getTime()/1000
  }

  /**
    * 切片分析
    *
    * @param args
    *             args(0) 指标名称
    *             args(1) 开始时间
    *             args(2) 结束时间
    *             args(3) 主机名
    *             args(4) 周期
    */
  def main(args: Array[String]) {

//    参数检查
//    if (args.length != 5) {
//      println("need 5 args,like: index_name,start_time,end_time,host_name,period")
//      System.exit(1)
//    }
    val sc = SparkContextUtil.getInstance()
    val props = ItoaPropertyUtil.getProperties()

    val index_name = args(0)//指标名称
    val start_time = getDate2Seconds(args(1))//开始时间
    val end_time = getDate2Seconds(args(2))//结束时间
    val host_name = args(3)//主机名
    val period = args(4)//周期

    val modelObjKey = index_name + "_"+ period + "_" + host_name
    val indexNamePeriod = index_name + "_"+ period


//    val map=Thread4InitialModelObj.getModelMap()
//    模型加载
    val map = new ConcurrentHashMap[String, AllPredcictTarget]
    var obj: AllPredcictTarget = new AllPredcictTarget(null, null)
    try {
      obj = new AllPredcictTarget(indexNamePeriod, host_name)
    } catch {
      case e => logger.error("** " + modelObjKey + " NEW_ERROR ** =>new AllPredcictTarget(" + indexNamePeriod + "," + host_name + ") :ExceptionMsg:" + e.getMessage + "\n" + e.printStackTrace())
    }
    map.put(modelObjKey, obj)

    val initial = new InitUtil(props, map)
    //设置为广播变量
    val initialBroadCast = sc.broadcast(initial)

//    获取mysql数据库中的某张表的dataframe
    val hiveCtx = new HiveContext(sc)
    val indexDF = Thread4GAACPredAna.getHostConfig(hiveCtx, props, "T_TABLE_HOST_INDEX")

//    测试代码
//    val sqlCtx = new SQLContext(sc)
//    val dburl = "jdbc:mysql://localhost/wsd?user=training&password=training"
//    val indexDF=sqlCtx.load("jdbc",
//      Map("url"-> dburl,
//        "dbtable" -> "T_TABLE_HOST_INDEX"))

//    查询目标指标对应的表名 及数据库名
    indexDF.registerTempTable("TMP_TABLE_HOST_INDEX")
    val indexTableName = hiveCtx.sql("SELECT tablename FROM TMP_TABLE_HOST_INDEX " +
      "where index_name ='" + index_name +"'").first()(0).toString
    logger.info("==============indexTableName=============="+ indexTableName)

    //测试使用itm.system表
    val ssa_data = hiveCtx.sql("select * from " + args(5) +
      " where system_name like '%" + host_name + "%'" +
      " and unix_timestamp(concat( '20', substring(dwritetime, 2, 10)),'yyyyMMddHHmm') > " + start_time +
      " and unix_timestamp(concat( '20', substring(dwritetime, 2, 10)),'yyyyMMddHHmm') < " + end_time)


    val sys_cur_time :Long= System.currentTimeMillis() / 1000
    if ( ssa_data.count()!= 0) {
      val ssa_res = ssa_data.map(row => {
//        调用相关分析接口
        val pre_res = RealTimeDataProcessingNew.dataProcessing(initialBroadCast.value, "system", row.toString(), "relationAnalysis")

        val resObject = new util.HashMap[String, Object]()
        resObject.put(SysConst.MAP_DATA_GETTIME_KEY, sys_cur_time.toString)
        resObject.put(SysConst.MAP_DATA_RESULT_KEY, pre_res)
        resObject
      })
      val ssa_res_list = ssa_res.collect().toList
      logger.info("=====================pre_res=======================")
      logger.info(ssa_res_list)
//      切片分析结果保存
      new PredResultService().dataRelationAnalysisSave(ssa_res_list)
    }else{
      logger.warn("=====================ssa_data.count()=" + ssa_data.count() + "=====================")
    }
    sc.stop()
  }
}
