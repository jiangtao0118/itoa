package com.wisdom.spark.streaming.dao

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.streaming.bean.ModelObject
import com.wisdom.spark.streaming.tools.ConnUtils
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2017/1/18.
  * 模型对象dao操作，mysql保存和查询
  */
class ModelObjDao {
  val props = ItoaPropertyUtil.getProperties()
  val logger = Logger.getLogger(this.getClass)

  /**
    * 保存模型对象到mysql数据库
    *
    * @param modelObj 模型对象
    * @param conn     数据库连接
    */
  def saveModelObj2Mysql(modelObj: ModelObject, conn: Connection): Unit = {
    if (modelObj != null) {
      var pst: PreparedStatement = null
      val sql = "insert into t_model_object ( modelObjKey,predIndexName,predPeriod,hostName,preCol,isValid) " +
        "values (?,?,?,?,?,?)"
      try {
        pst = conn.prepareStatement(sql)
        pst.setString(1, modelObj.getModelObjKey)
        pst.setString(2, modelObj.getPredIndexName)
        pst.setString(3, modelObj.getPredPeriod)
        pst.setString(4, modelObj.getHostName)
        pst.setString(5, modelObj.getPreCol)
        pst.setString(6, modelObj.getIsValid)
        pst.executeUpdate()
        conn.commit()
      } catch {
        case e: Exception => logger.error("SQL Exception!!! PreparStatement executed Failed!!! FUNC:saveModelObj2Mysql(modelObj: ModelObject, conn: Connection)" + e.getMessage)
      } finally {
        ConnUtils.closeStatement(null, pst, null)
      }
    }
  }

  /**
    * 批量保存模型对象至mysql数据库
    *
    * @param list 关联分析结果list
    * @param conn 数据库连接
    */
  def saveModelObjList2Mysql(list: List[ModelObject], conn: Connection): Unit = {
    if (list != null && list.size != 0) {
      var pst: PreparedStatement = null
      val batchNums = props.getProperty("mysql.sql.batch").toInt
      val sql = "insert into t_model_object ( modelObjKey,predIndexName,predPeriod,hostName,preCol,isValid) " +
        "values (?,?,?,?,?,?)"
      try {
        pst = conn.prepareStatement(sql)
        for (i <- 0 until list.length) {
          pst.setString(1, list(i).getModelObjKey)
          pst.setString(2, list(i).getPredIndexName)
          pst.setString(3, list(i).getPredPeriod)
          pst.setString(4, list(i).getHostName)
          pst.setString(5, list(i).getPreCol)
          pst.setString(6, list(i).getIsValid)
          pst.addBatch()
          if (i % batchNums == 0) {
            pst.executeBatch()
          }
        }
        pst.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => logger.error("SQL Exception!!! PreparStatement executed Failed!!! FUNC:saveModelObjList2Mysql(list: List[ModelObject], conn: Connection)" + e.getMessage)
      } finally {
        ConnUtils.closeStatement(null, pst, null)
      }
    }
  }

  /**
    * 清空模型对象表，便于重新初始化
    *
    * @param conn 数据库连接
    */
  def truncateModelObj(conn: Connection): Unit = {
    val sql = "delete from t_model_object"
    var st: Statement = null
    try {
      st = conn.createStatement()
      st.executeUpdate(sql)
      conn.commit()
    } catch {
      case e: Exception => logger.error("SQL Exception!!! Statement executed Failed!!! FUNC:truncateModelObj(conn: Connection)" + e.getMessage)
    } finally {
      ConnUtils.closeStatement(st, null, null)
    }

  }

  /**
    * 查询所有的模型对象以list结果集返回
    *
    * @param conn
    * @return
    */
  def findModelObjAll(conn: Connection): util.List[ModelObject] = {
    val list = new util.ArrayList[ModelObject]
    val sql = props.getProperty("model.init.sql")
    var st: Statement = null
    var rs: ResultSet = null
    try {
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      var modelObj: ModelObject = null
      while (rs.next()) {
        modelObj = new ModelObject()
        modelObj.setModelId(rs.getInt(1))
        modelObj.setModelObjKey(rs.getString(2))
        modelObj.setPredIndexName(rs.getString(3))
        modelObj.setPredPeriod(rs.getString(4))
        modelObj.setHostName(rs.getString(5))
        modelObj.setPreCol(rs.getString(6))
        modelObj.setIsValid(rs.getString(7))
        list.add(modelObj)
      }
    } catch {
      case e: Exception => logger.error("SQL Exception!!! Statement executed Failed!!! FUNC:findModelObjAll(conn: Connection)" + e.getMessage)
    } finally {
      ConnUtils.closeStatement(st, null, rs)
    }
    list
  }

}
