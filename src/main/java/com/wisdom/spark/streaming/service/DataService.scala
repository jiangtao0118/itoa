package com.wisdom.spark.streaming.service

import com.wisdom.spark.streaming.tools.{ConnPoolUtil, FileUtils, ParamUtil}
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.util
import java.util.Date

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import org.apache.log4j.Logger

/**
  * Created by wisdom on 2016/11/30.
  * 实时数据处理业务层方法（第一版中字符串形式的预测信息中使用过，目前已经淘汰，暂时不使用）
  */
@Deprecated
class DataService extends Serializable {
  val logger = Logger.getLogger(this.getClass)
  val props = ItoaPropertyUtil.getProperties()
  var conn: Connection = null
  var st: Statement = null
  var rs: ResultSet = null

  /**
    * 处理预警结果数据，保存至Mysql
    *
    * @param result
    */
  def doPredResultSaveMySQL(result: Map[String, Any]): Unit = {
    val l: List[Map[String, Any]] = List(result)
    doPredResultSaveMySQL(l)
  }

  /**
    * 批量处理预警结果数据，保存至MySQL
    *
    * @param resultList
    */
  def doPredResultSaveMySQL(resultList: List[Map[String, Any]]): Unit = {
    try {
      //      conn = ConnPoolUtil.getConnection.getOrElse(null)
      conn = ConnPoolUtil2.getConn()
      st = conn.createStatement()
      conn.setAutoCommit(false)
      val sqlList = appendInsertSql(resultList).split(";")
      for (sql <- sqlList) {
        logger.info("SQL->" + sql)
        st.addBatch(sql)
      }
      st.executeBatch()
      conn.commit()
    } catch {
      //      case e: Exception => logger.error("SQL-insert-Exception" + e.printStackTrace())
      case e: Exception => logger.error("SQL-insert-Exception")
    } finally {
      try {
        if (st != null && (!st.isClosed)) st.close()
      } catch {
        //        case e: Exception => logger.error("SQL-close-Exception" + e.printStackTrace())
        case e: Exception => logger.error("SQL-close-Exception")
      } finally {
        ConnPoolUtil2.releaseCon(conn)
      }
    }
  }

  /**
    * 拼接SQL - insert语句
    *
    * @param batchValue
    * @return
    */
  def appendInsertSql(batchValue: List[Map[String, Any]]): String = {
    val cache = props.getProperty("mysql.sql.batch").toInt
    val allRows = batchValue.length
    val batchSql = new StringBuilder("")
    if (allRows > 0) {
      //batch循环次数
      var j = 0
      val sqlPrx = "insert into t_pred_result(sysName,hostName,predIndexName,currentTime,indexTyp,predPeriod,dataAcceptTime,currentDataTime,currentActualValue,nextPredValue,nextPredMinValue,nextPredMaxValue) values "
      val sql = new StringBuilder(sqlPrx)
      for (i <- 0 to allRows - 1) {
        if (j < cache) {
          j = j + 1
        } else {
          j = 1
          batchSql.append(sql.deleteCharAt(sql.length - 1)).append(";")
          sql.setLength(0)
          sql.append(sqlPrx)
        }
        sql.append("(")

          .append(if (batchValue(i).contains("sysName")) batchValue(i)("sysName") else "").append("','")
          .append(if (batchValue(i).contains("hostName")) batchValue(i)("hostName") else "").append("','")
          .append(if (batchValue(i).contains("predIndexName")) batchValue(i)("predIndexName") else "").append("','")
          .append(if (batchValue(i).contains("currentTime")) batchValue(i)("currentTime") else "").append("','")
          .append(if (batchValue(i).contains("indexTyp")) batchValue(i)("indexTyp") else "").append("','")
          .append(if (batchValue(i).contains("predPeriod")) batchValue(i)("predPeriod") else "").append("','")
          .append(if (batchValue(i).contains("dataAcceptTime")) batchValue(i)("dataAcceptTime") else "").append("','")
          .append(if (batchValue(i).contains("currentDataTime")) batchValue(i)("currentDataTime") else "").append(",'")
          .append(if (batchValue(i).contains("currentActualValue")) batchValue(i)("currentActualValue") else "").append("','")
          .append(if (batchValue(i).contains("nextPredValue")) batchValue(i)("nextPredValue") else "").append("','")
          .append(if (batchValue(i).contains("nextPredMinValue")) batchValue(i)("nextPredMinValue") else "").append("','")
          .append(if (batchValue(i).contains("nextPredMaxValue")) batchValue(i)("nextPredMaxValue") else "")
          .append("'),")
        //        println("i:" + i + "\tj:" + j + "\tsql:" + sql + "\tbatchSQL:" + batchSql)
      }
      batchSql.append(sql.deleteCharAt(sql.length - 1))
    }
    batchSql.toString()

  }

  /**
    * 处理预警结果数据，保存到文件
    *
    * @param result
    */
  def doPredResultSaveFile(result: Map[String, Any]): Unit = {
    val l: List[Map[String, Any]] = List(result)
    doPredResultSaveFile(l)
  }

  /**
    * 批量处理预警结果数据，保存到文件
    *
    * @param resultList
    */
  def doPredResultSaveFile(resultList: List[Map[String, Any]]): Unit = {
    val fileDir: String = props.getProperty("data.itm.file.dir")
    val fileName: String = props.getProperty("data.itm.file.namespace") + "_" + new Date().getTime
    try {
      FileUtils.write2Path(fileDir, fileName, appendFileContent(resultList), false)
    } catch {
      //      case e: Exception => logger.error("File-write-Exception" + e.printStackTrace())
      case e: Exception => logger.error("File-write-Exception")
    }
  }

  /**
    * 预警文件拼接为行数据
    *
    * @param resultList
    * @return
    */
  def appendFileContent(resultList: List[Map[String, Any]]): String = {
    //predId,sysName,hostName,predIndexName,currentTime,indexTyp,predPeriod,
    // dataAcceptTime,currentDataTime,currentActualValue,nextPredValue,nextPredMinValue,nextPredMaxValue
    val fileContent: StringBuilder = new StringBuilder()
    for (str <- resultList) {
      fileContent.append(if (str.contains("predId")) str("predId") else -1).append(",")
        .append(if (str.contains("sysName")) str("sysName") else "").append(",")
        .append(if (str.contains("hostName")) str("hostName") else "").append(",")
        .append(if (str.contains("predIndexName")) str("predIndexName") else "").append(",")
        .append(if (str.contains("currentTime")) str("currentTime") else "").append(",")
        .append(if (str.contains("indexTyp")) str("indexTyp") else "").append(",")
        .append(if (str.contains("predPeriod")) str("predPeriod") else "").append(",")
        .append(if (str.contains("dataAcceptTime")) str("dataAcceptTime") else "").append(",")
        .append(if (str.contains("currentDataTime")) str("currentDataTime") else "").append(",")
        .append(if (str.contains("currentActualValue")) str("currentActualValue") else "").append(",")
        .append(if (str.contains("nextPredValue")) str("nextPredValue") else "").append(",")
        .append(if (str.contains("nextPredMinValue")) str("nextPredMinValue") else "").append(",")
        .append(if (str.contains("nextPredMaxValue")) str("nextPredMaxValue") else "")
        .append("\n")
    }
    fileContent.toString()
  }

  /**
    * 将预测结果保存至MySQL，该预测结果是逗号分隔子表字符串，需拆分处理
    *
    * 数据是字符串形式，需要进行逗号分隔
    *
    * @param res
    */
  def doPredResultOfStrSaveMySQL(res: List[util.HashMap[String, String]]): Unit = {
    logger.info("************  Here is DataService.doPredResultOfStrSaveMySQL  **********************")
    logger.info("************  Input:" + res)
    if (!res.isEmpty && res.length != 0) {
      try {
        //        conn = ConnPoolUtil.getConnection.getOrElse(null)
        conn = ConnPoolUtil2.getConn()
        st = conn.createStatement()
        conn.setAutoCommit(false)
        val sqlList = appendInsertSqlOfStr(res).split(";")
        for (sql <- sqlList) {
          logger.info("SQL->" + sql)
          st.addBatch(sql)
        }
        st.executeBatch()
        conn.commit()
      } catch {
        //        case e: Exception => logger.error("SQL-insert-Exception" + e.printStackTrace())
        case e: Exception => logger.error("SQL-insert-Exception")
      } finally {
        try {
          if (st != null && (!st.isClosed)) st.close()
        } catch {
          //          case e: Exception => logger.error("SQL-close-Exception" + e.printStackTrace())
          case e: Exception => logger.error("SQL-close-Exception")
        } finally {
          ConnPoolUtil2.releaseCon(conn)
        }
      }
    }
  }

  /**
    * 拆分预测结果值，并拼接为SQL查询子句返回
    *
    * 预测结果是逗号分隔形式，需拆分
    *
    * @param batchValue
    * @return
    */
  def appendInsertSqlOfStr(batchValue: List[util.HashMap[String, String]]): String = {
    val cache = props.getProperty("mysql.sql.batch").toInt
    val allRows = batchValue.length
    val batchSql = new StringBuilder("")
    if (allRows > 0) {
      //batch循环次数
      var j = 0
      val sqlPrx = "insert into t_pred_result(sysName,hostName,predIndexName,currentTime,indexTyp,predPeriod,dataAcceptTime,currentDataTime,currentActualValue,nextPredValue,nextPredMinValue,nextPredMaxValue) values "
      val sql = new StringBuilder(sqlPrx)
      for (i <- 0 until allRows) {
        if (j < cache) {
          j = j + 1
        } else {
          j = 1
          batchSql.append(sql.deleteCharAt(sql.length - 1)).append(";")
          sql.setLength(0)
          sql.append(sqlPrx)
        }
        val body = batchValue(i).get("result").split(",", -1)
        //sysName,hostName,predIndexName,currentTime,indexTyp,predPeriod,dataAcceptTime,currentDataTime,currentActualValue,nextPredValue,nextPredMinValue,nextPredMaxValue
        //系统名,主机名,预测指标名称,(SystemDate),指标类型,预测周期,(SystemDate),数据时刻,当前时刻实测值,下一时刻预测值,下一时刻预测下限,下一时刻预测上限
        if (body.length == 10) {
          sql.append("('")
            .append(body(0)).append("','")
            .append(body(1)).append("','")
            .append(body(2)).append("','")
            .append(System.currentTimeMillis()).append("','")
            .append(body(3)).append("','")
            .append(body(4)).append("','")
            .append(batchValue(i).get("dataGetTime")).append("','")
            .append(body(5)).append("','")
            .append(body(6)).append("','")
            .append(body(7)).append("','")
            .append(body(8)).append("','")
            .append(body(9))
            .append("'),")
        }
      }
      batchSql.append(sql.deleteCharAt(sql.length - 1))
    }
    batchSql.toString()
  }
}
