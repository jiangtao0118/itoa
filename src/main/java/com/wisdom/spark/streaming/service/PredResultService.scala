package com.wisdom.spark.streaming.service

import java.sql.Connection
import java.util

import com.wisdom.spark.common.ExecutorState
import com.wisdom.spark.common.bean.{PredResult, RelationAnalysis}
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.bean.AlarmResult
import com.wisdom.spark.streaming.dao.{PredResultDao, RelationDao}
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import com.wisdom.spark.streaming.tools._
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2017/1/3.
  * 预测结果处理业务逻辑service，主要负责将实时接收的数据进行模型预测完的结果数据和关联分析结果数据，解析为list，
  * 批量保存到预测结果表和关联分析表，并且针对预测结果进行告警判断过程，调用告警service，如果发生告警则保存数据库和写文件
  */
class PredResultService extends Serializable {
  //  日志类
  val logger = Logger.getLogger(this.getClass)
  val predResultDao = new PredResultDao()
  val relationDao = new RelationDao()
  val props = ItoaPropertyUtil.getProperties()

//  val conn = ConnPoolUtil2.getConn()

  /**
    * 将预测结果和关联分析结果分别保存保存数据库并且判断是否存在告警
    *
    * @param resList 预测结果和关联分析结果
    *                eg:List({dataResult=Map(predResult -> List(PredResult(predId=null, sysName=system, hostName=ASCECUP01, predIndexName=Idle_CPU, currentTime=, indexTyp=00, predPeriod=15, dataAcceptTime=, currentDataTime=1481685893, currentActualValue=, nextPredValue=, nextPredMinValue=, nextPredMaxValue=, executorState=301:忽略此主机))),
    *                         dataGetTime=1483429870})
    */
  def dataAcceptAndSaveMysql(resList: List[util.HashMap[String, Object]]): Unit = {
    logger.warn("************** dataAcceptAndSaveMysql预测结果保存MYSQL *****************")
    //数据库连接
    try {
      //用于保存"成功"的预测结果的List
      var listPredResult: List[PredResult] = List()
      /**
        * 当结果集resList不为空时,保存预测结果到MySQL预测结果表t_pred_result中，并判断是否告警，
        * 如果告警，将告警结果保存到MySQL告警结果表t_alarm_result中
        */
      if (resList != null && resList.size != 0) {
        //mysql中所需字段：接收到数据的时间
        var dataGetTime: String = ""
        //用于承载预测结果的Map
        var dataResult: Map[String, List[Any]] = null
        //循环迭代封装好的预测结果对象集合
        for (resObject <- resList) {
          //判断resObject中是否包含数据接收时间dataGetTime，如果有则获取
          if (resObject.containsKey(SysConst.MAP_DATA_GETTIME_KEY)) {
            dataGetTime = resObject.get(SysConst.MAP_DATA_GETTIME_KEY).toString
          }
          //判断resObject中是否包含预测结果dataResult，并且类型是否是Map，如果有则按Map类型获取
          if (resObject.containsKey(SysConst.MAP_DATA_RESULT_KEY) && resObject.get(SysConst.MAP_DATA_RESULT_KEY).isInstanceOf[Map[String, List[Any]]]) {
            dataResult = resObject.get(SysConst.MAP_DATA_RESULT_KEY).asInstanceOf[Map[String, List[Any]]]
          }
          //如果resObject中包含dataResult，且内容不为空，那么读取dataResult这个Map当中的内容
          if (dataResult != null) {
            //如果dataResult中的内容为预测结果predResult（非关联分析结果relationAnalysis）
            //（预测结果predResult以List[PredResult]的格式存储在dataResult中）
            if (dataResult.contains(SysConst.MAP_PRED_RESULT_KEY) && dataResult(SysConst.MAP_PRED_RESULT_KEY).isInstanceOf[List[PredResult]]) {
              //那么获取predResult键的值，即预测结果
              val predRes = dataResult(SysConst.MAP_PRED_RESULT_KEY).asInstanceOf[List[PredResult]]
              //遍历List[PredResult]
              for (pred <- predRes) {
                //如果预测结果中的执行状态为"成功"，那么将数据接收时间dataGetTime插入预测结果类PredResult中
                if (pred.getExecutorState == ExecutorState.success) {
                  //在预测结果中添加数据接收的时间
                  pred.setDataAcceptTime(dataGetTime.toString)
                  //将成功预测的结果存入listPredResult中
                  listPredResult :+= pred
                }
              }
            }
          }
        }
        //如果有"成功"预测的结果，那么保存预测结果，并判断是否告警
        if (listPredResult.size != 0) {
          //批量保存预测结果开始时间
          val t1 = System.currentTimeMillis()
          //批量保存预测结果
          val size=listPredResult.size
          val batch=props.getProperty("mysql.sql.batch").toInt
          val loops=size/batch +1
          for(i<-0 until loops){
            var batchListPPN: List[PredResult] = List()
            var batchListWINSTATS: List[PredResult] = List()
            var batchListAll: List[PredResult] = List()
            var flag=true
            for(j<-i*batch until size if flag){
              if(j<(i+1)*batch ){
                val predResult=listPredResult(j)
                batchListAll:+=predResult
                if(props.getProperty("ml.model_type.WINSTATS").equalsIgnoreCase(predResult.getModelType)){
                  batchListWINSTATS:+=predResult
                }else if(props.getProperty("ml.model_type.PPN").equalsIgnoreCase(predResult.getModelType)){
                  batchListPPN:+=predResult
                }
              }else{
                flag=false
              }
            }
//            ThreadPools.getPools().execute(new Thread4SavePPNResult(batchListPPN))
            ThreadPools.getPools().execute(new Thread4SaveWINSTATSResult(batchListWINSTATS))
//            ThreadPools.getPools().execute(new Thread4CheckAlarmPPNBatch(batchListPPN))
            ThreadPools.getPools().execute(new Thread4CheckAlarmWINSTATSBatch(batchListWINSTATS))
          }
        }
      }
    } catch {
      case e: Exception => logger.error("**** 预测结果处理逻辑存在异常(告警、保存) ****" + e.printStackTrace())
    }
  }

  /**
    * 关联分析结果保存mysql
    *
    * @param resList 预测结果和关联分析结果List
    */
  def dataRelationAnalysisSave(resList: List[util.HashMap[String, Object]]): Unit = {
    logger.info("************** 关联分析结果保存MYSQL *****************")
    try {
      var listRealtionAna: List[RelationAnalysis] = List()
      if (resList != null && resList.size != 0) {
        var dataGetTime: String = ""
        var dataResult: Map[String, List[Any]] = null
        for (resObject <- resList) {
          if (resObject.containsKey(SysConst.MAP_DATA_GETTIME_KEY)) {
            dataGetTime = resObject.get(SysConst.MAP_DATA_GETTIME_KEY).toString
          }
          if (resObject.containsKey(SysConst.MAP_DATA_RESULT_KEY) && resObject.get(SysConst.MAP_DATA_RESULT_KEY).isInstanceOf[Map[String, List[Any]]]) {
            dataResult = resObject.get(SysConst.MAP_DATA_RESULT_KEY).asInstanceOf[Map[String, List[Any]]]
          }
          if (dataResult != null) {
            if (dataResult.contains(SysConst.MAP_RELATION_ANALYSIS_KEY) && dataResult(SysConst.MAP_RELATION_ANALYSIS_KEY).isInstanceOf[List[RelationAnalysis]]) {
              //关联分析结果
              val relaAna = dataResult(SysConst.MAP_RELATION_ANALYSIS_KEY).asInstanceOf[List[RelationAnalysis]]
              for (rela <- relaAna) {
                if (rela.getExecutorState == ExecutorState.success) {
                  listRealtionAna :+= rela
                }
              }
            }
          }
        }
        //保存关联分析结果
        if (listRealtionAna.size != 0) {
          //批量保存预测结果开始时间
          val t1 = System.currentTimeMillis()
          //批量保存预测结果
          val size=listRealtionAna.size
          val batch=props.getProperty("mysql.sql.batch").toInt
          val loops=size/batch +1
          for(i<-0 until loops){
            var batchListAll: List[RelationAnalysis] = List()
            var flag=true
            for(j<-i*batch until size if flag){
              if(j<(i+1)*batch ){
                val relationResult=listRealtionAna(j)
                batchListAll:+=relationResult
              }else{
                flag=false
              }
            }
            ThreadPools.getPools().execute(new Thread4SaveRelationResult(batchListAll))
          }
        }
      }
    } catch {
      case e: Exception => logger.error(e.printStackTrace())
    }
  }

  def testSaveList(listPredResult:List[PredResult])={
    if (listPredResult.size != 0) {
      //批量保存预测结果开始时间
      val t1 = System.currentTimeMillis()
      //批量保存预测结果
      val size=listPredResult.size
      val batch=props.getProperty("mysql.sql.batch").toInt
      val loops=size/batch +1
      for(i<-0 until loops){
        var batchListPPN: List[PredResult] = List()
        var batchListWINSTATS: List[PredResult] = List()
        var batchListAll: List[PredResult] = List()
        var flag=true
        for(j<-i*batch until size if flag){
          if(j<(i+1)*batch ){
            val predResult=listPredResult(j)
            batchListAll:+=predResult
            if(props.getProperty("ml.model_type.WINSTATS").equalsIgnoreCase(predResult.getModelType)){
              batchListWINSTATS:+=predResult
            }else if(props.getProperty("ml.model_type.PPN").equalsIgnoreCase(predResult.getModelType)){
              batchListPPN:+=predResult
            }
          }else{
            flag=false
          }
        }
//        ThreadPools.getPools().execute(new Thread4SavePPNResult(batchListPPN))
        ThreadPools.getPools().execute(new Thread4SaveWINSTATSResult(batchListWINSTATS))
//        ThreadPools.getPools().execute(new Thread4CheckAlarmBatch(batchListAll))
//        ThreadPools.getPools().execute(new Thread4CheckAlarmPPNBatch(batchListPPN))
//        ThreadPools.getPools().execute(new Thread4CheckAlarmWINSTATSBatch(batchListWINSTATS))
      }
    }
  }

}
