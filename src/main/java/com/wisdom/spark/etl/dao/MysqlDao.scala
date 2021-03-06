package com.wisdom.spark.etl.dao

import java.sql._
import java.util

import com.wisdom.spark.common.util.ConnPoolUtil2
import com.wisdom.spark.etl.bean.InitBean
import org.apache.log4j.Logger

/**
  * Created by wisdom on 2016/12/27.
  */
object MysqlDao {
  val logger = Logger.getLogger(this.getClass)
  var conn: Connection = null
  var pst: PreparedStatement = null

  //  def DBHelper(sql: String): util.ArrayList[String] = {
  //    val list = new util.ArrayList[String]()
  //    try {
  //      conn=ConnPoolUtil2.getConn()
  //      pst = conn.prepareStatement(sql)
  //      //准备执行语句
  //      val rs = pst.executeQuery()
  //
  //      if (rs != null) {
  //        while (rs.next()) {
  //          list.add(rs.getString(1).toLowerCase() + "_" + rs.getString(2).toUpperCase())
  //        }
  //      }
  //      list
  //    } catch {
  //      case ex: Exception => logger.error("DBHelper Exception" + ex.printStackTrace())
  //        list
  //    } finally {
  //      ConnPoolUtil2.releaseCon(conn)
  //    }
  //  }
  /**
    * 初始化要做的指标
    *
    * @param sql
    * @return
    */
  def initBean(sql: String): util.ArrayList[InitBean] = {
    val list = new util.ArrayList[InitBean]()
    try {
      conn = ConnPoolUtil2.getConn()
      pst = conn.prepareStatement(sql)
      //准备执行语句
      val rs = pst.executeQuery()
      //TODO 修改为从其它表查出所需信息，方便更新主机等信息

      if (rs != null) {
        while (rs.next()) {
          val hosts = rs.getString(4).split(",")
          for (i <- 0 until hosts.length) {
            val initBean = new InitBean
            initBean.setHostName(hosts(i).toUpperCase())
            initBean.setSystemName(rs.getString(2).toUpperCase())
            initBean.setTargetName(rs.getString(3).toUpperCase())
            list.add(initBean)
          }
        }
      }
      list
    } catch {
      case ex: Exception => logger.error("SQL Exception" + ex.printStackTrace())
        list
    } finally {
      ConnPoolUtil2.releaseCon(conn)
    }
  }

  /**
    * 初始化要做的指标
    *
    * @param sql
    * @return
    */
  def selectBean(sql: String): util.ArrayList[InitBean] = {
    val list = new util.ArrayList[InitBean]()
    try {
      conn = ConnPoolUtil2.getConn()
      pst = conn.prepareStatement(sql)
      //准备执行语句
      val rs = pst.executeQuery()
      //TODO 修改为从其它表查出所需信息，方便更新主机等信息

      if (rs != null) {
        while (rs.next()) {
          //          val hosts=rs.getString(4).split(",")
          val initBean = new InitBean
          initBean.setHostName(rs.getString(4).toUpperCase())
          initBean.setSystemName(rs.getString(2).toUpperCase())
          initBean.setTargetName(rs.getString(3).toUpperCase())
          list.add(initBean)
        }
      }
      list
    } catch {
      case ex: Exception => logger.error("SQL Exception" + ex.printStackTrace())
        list
    } finally {
      ConnPoolUtil2.releaseCon(conn)
    }
  }

}
