package com.wisdom.spark.streaming.dao

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util
import java.util.Date

import com.wisdom.spark.common.bean.RelationAnalysis
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import com.wisdom.spark.streaming.tools.{ConnUtils, ParamUtil}
import org.apache.log4j.Logger

/**
  * Created by wisdom on 2016/12/29.
  * 告警关联分析表dao操作，数据保存和查询
  */
class RelationDao {
  val logger = Logger.getLogger(this.getClass)

  /**
    * 保存关联分析结果到mysql数据库
    *
    * @param relationAnalysis 关联分析结果
    * @param conn             数据库连接
    */
  def saveRelationAnalysis2Mysql(relationAnalysis: RelationAnalysis, conn: Connection): Unit = {
    if (relationAnalysis != null) {
      var pst: PreparedStatement = null
      val sql = "insert into t_relation_analysis_result ( sysName, hostName, predIndexName, indexTyp, predPeriod, currentDataTime, analysisResult) " +
        "values (?,?,?,?,?,?,?)"
      try {
        pst = conn.prepareStatement(sql)
        pst.setString(1, relationAnalysis.getSysName)
        pst.setString(2, relationAnalysis.getHostName)
        pst.setString(3, relationAnalysis.getPredIndexName)
        pst.setString(4, relationAnalysis.getIndexTyp)
        pst.setString(5, relationAnalysis.getPredPeriod)
        pst.setString(6, relationAnalysis.getCurrentDataTime)
        pst.setString(7, relationAnalysis.getAnalysisResult)
        pst.executeUpdate()
        conn.commit()
      } catch {
        case e: Exception => logger.error("SQL Exception!!! PreparStatement executed Failed!!! FUNC:saveRelationAnalysis2Mysql(relationAnalysis: RelationAnalysis, conn: Connection)" + e.getMessage)
      } finally {
        if (pst != null && (!pst.isClosed)) {
          try {
            pst.close()
          } catch {
            case e: Exception => logger.error("SQL Exception!!! PrepareStatement closed Failed!!!" + e.getMessage)
          }
        }
      }
    }
  }

  /**
    * 批量保存关联分析结果至mysql数据库
    *
    * @param list 关联分析结果list
    * @param conn 数据库连接
    */
  def saveRelationAnalysisList2Mysql(list: List[RelationAnalysis]): Unit = {
    if (list != null && list.size != 0) {
      var conn: Connection = null
      var pst: PreparedStatement = null
      val sql = "insert into t_relation_analysis_result ( sysName, hostName, predIndexName, indexTyp, predPeriod, currentDataTime, analysisResult) " +
        "values (?,?,?,?,?,?,?)"
      try {
        conn=ConnPoolUtil2.getConn()
        pst = conn.prepareStatement(sql)
        for (i <- 0 until list.length) {
          pst.setString(1, list(i).getSysName)
          pst.setString(2, list(i).getHostName)
          pst.setString(3, list(i).getPredIndexName)
          pst.setString(4, list(i).getIndexTyp)
          pst.setString(5, list(i).getPredPeriod)
          pst.setString(6, list(i).getCurrentDataTime)
          pst.setString(7, list(i).getAnalysisResult)
          pst.addBatch()
        }
        pst.executeBatch()
      } catch {
        case e: Exception => logger.error("SQL Exception!!! PreparStatement executed Failed!!! FUNC:saveRelationAnalysis2Mysql(relationAnalysis: RelationAnalysis, conn: Connection)" + e.getMessage)
      } finally {
        if (conn != null) {
          //释放数据库连接
          ConnPoolUtil2.releaseCon(conn)
        }
        ConnUtils.closeStatement(pst,null , null)
      }
    }
  }

  /**
    * 查询所有的预测指标的数据源和文件名：itm.system  opm.opm_db
    *
    * @param conn 数据库连接
    * @return 数据源信息
    */
  def findDataSourceList(conn: Connection): List[util.HashMap[String, String]] = {
    /* -------  暂时没有保存数据库默认读配置文件方式  -------*/
    null
  }
}
