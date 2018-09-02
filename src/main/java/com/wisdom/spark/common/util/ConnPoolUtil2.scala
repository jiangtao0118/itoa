package com.wisdom.spark.common.util

import java.sql.{Connection, DriverManager}
import java.util

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.streaming.tools.ParamUtil
import org.apache.log4j.Logger


/**
  * Created by wisdom on 2016/12/13.
  */
object ConnPoolUtil2 {
  val props = ItoaPropertyUtil.getProperties()
  val logger = Logger.getLogger(this.getClass)

  private val maxConnections = props.getProperty("mysql.pool.max").toInt
  private val connectionNums = props.getProperty("mysql.pool.nums").toInt
  private var currConnNums = 0
  private val pools = new util.LinkedList[Connection]()
  private val driver = props.getProperty("mysql.db.driver")
  private val url = props.getProperty("mysql.db.url")
  private val userName = props.getProperty("mysql.db.user")
  private val userPwd = props.getProperty("mysql.db.pwd")


  /**
    * 加载驱动
    */
  private def before() {
    if (currConnNums > maxConnections.toInt && pools.isEmpty()) {
      logger.warn("Connection's pool is busyness in this time!!!")
      Thread.sleep(10000)
      before()
    } else {
      Class.forName(driver)
    }
  }

  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, userName, userPwd)
    conn
  }

  /**
    * 初始化连接池
    */
  private def initConnectionPool(): util.LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connectionNums) {
          pools.push(initConn())
          currConnNums += 1
        }
      }
      //      logger.info("PoolsInfo==>" + pools)
      pools
    })
  }

  /**
    * 获得连接
    */
  def getConn(): Connection = {
    var conn: Connection = null
    this.synchronized {
      initConnectionPool()
      conn = pools.poll()
      if (conn == null || conn.isClosed) {
        pools.remove(conn)
        conn = initConn()
      }
    }
    conn
  }

  /**
    * 释放连接
    */
  def releaseCon(con: Connection) {
    this.synchronized(pools.push(con))
  }
}
