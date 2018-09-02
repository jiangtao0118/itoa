package com.wisdom.spark.streaming.tools

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2016/11/29.
  * 采用BoneCP依赖jar包创建数据库连接池（暂时不适用）
  */
@Deprecated
object ConnPoolUtil {
  val logger = Logger.getLogger(this.getClass)
  val props = ItoaPropertyUtil.getProperties()
  /**
    * 定义数据库连接池
    *
    * JDBCdriver:数据库连接驱动
    * JDBCurl:数据库连接URL地址
    * JDBCuserName:数据库连接用户名
    * JDBCpassword:数据库连接用户密码
    * PartitionCount:连接池分区个数，默认3个
    * MaxConnectionsPerPartition:连接池每个分区最大的连接个数，默认4
    * MinConnectionsPerPartition:连接池每个分区最小的连接个数，默认1
    * AcquireIncrement:连接池中连接增长个数，默认1
    * PoolAvailabilityThreshold:连接池中合计最大连接数限制，默认30
    * ConnectionTimeout:数据库连接获取超时限制，默认30S
    * LazyInit:连接池初始化是否为null，默认true
    * CloseConnectionWatch:是否监控数据库连接关闭状态，默认true
    * LogStatementsEnabled:是否日志中打印SQL信息，默认true
    *
    */
  private val connectionPool = {
    try {
      Class.forName(props.getProperty("mysql.db.driver"))
      val conf = new BoneCPConfig()
      conf.setJdbcUrl(props.getProperty("mysql.db.url"))
      conf.setUsername(props.getProperty("mysql.db.user"))
      conf.setPassword(props.getProperty("mysql.db.pwd"))
      conf.setPartitionCount(props.getProperty("pool.bonecp.partitioncount").toInt)
      conf.setMaxConnectionsPerPartition(props.getProperty("pool.bonecp.maxperpartition").toInt)
      conf.setMinConnectionsPerPartition(props.getProperty("pool.bonecp.minperpartition").toInt)
      conf.setAcquireIncrement(props.getProperty("pool.bonecp.connincrement").toInt)
      conf.setPoolAvailabilityThreshold(props.getProperty("pool.bonecp.connmaxabled").toInt)
      //    conf.setConnectionTimeout(props.getProperty("pool.bonecp.conntimeout").toLong)
      conf.setLazyInit(props.getProperty("pool.bonecp.lazyinit").toBoolean)
      conf.setCloseConnectionWatch(props.getProperty("pool.boncp.connwatch").toBoolean)
      conf.setLogStatementsEnabled(props.getProperty("pool.boncp.logstate").toBoolean)
      Some(new BoneCP(conf))
    } catch {
      case e: Exception => logger.warn("Error in creation of connection pool!!!" + e.printStackTrace())
        None
    }

  }

  /**
    * 获取数据库连接存放Option
    *
    * @return
    */
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  /**
    * 关闭数据库连接
    *
    * @param connection
    */
  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      try {
        connection.close()
      } catch {
        case e: Exception => logger.warn("Error in closing of connection!!!" + e.printStackTrace())
      }
    }
  }
}