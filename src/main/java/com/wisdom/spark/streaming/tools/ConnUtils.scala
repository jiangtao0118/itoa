package com.wisdom.spark.streaming.tools

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2016/12/13.
  * 获取单个数据库连接，以及关闭数据库连接的静态方法
  */
object ConnUtils {
  val props = ItoaPropertyUtil.getProperties()
  val logger = Logger.getLogger(this.getClass)

  val driver = props.getProperty("mysql.db.driver")
  val url = props.getProperty("mysql.db.url")
  val userName = props.getProperty("mysql.db.user")
  val userPwd = props.getProperty("mysql.db.pwd")

  /**
    * 获取数据库连接
    *
    * @return
    */
  def getConn(): Connection = {
    Class.forName(driver)
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, userName, userPwd)
    } catch {
      case e: Exception => logger.error("ERROR:Getting connection failed!!!" + e.printStackTrace())
    }
    conn
  }

  /**
    * 关闭数据库连接
    *
    * @param conn
    */
  def closeConn(conn: Connection): Unit = {
    if (conn != null && (!conn.isClosed)) {
      conn.close()
    }
  }

  /**
    * 关闭相关数据库操作对象
    *
    * @param st
    * @param pst
    * @param rs
    */
  def closeStatement(st: Statement, pst: PreparedStatement, rs: ResultSet): Unit = {
    try {
      if (rs != null && (!rs.isClosed)) {
        rs.close()
      }
    } finally {
      try {
        if (pst != null && (!pst.isClosed)) {
          pst.close()
        }
      } finally {
        try {
          if (st != null && (!st.isClosed)) {
            st.close()
          }
        }
      }
    }

  }
}
