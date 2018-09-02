package com.wisdom.spark.streaming.service

import java.sql.{Connection, SQLException}
import java.util

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.ml.mlUtil.HDFSFileUtil
import com.wisdom.spark.streaming.bean.ModelObject
import com.wisdom.spark.streaming.dao.ModelObjDao
import org.apache.log4j.Logger

/**
  * Created by zhengz on 2017/1/18.
  * 处理模型对象数据，对表数据进行初始化以及查询操作
  */
class ModelObjService {
  //配置文件对象
  val props = ItoaPropertyUtil.getProperties()
  //日志操作对象
  val logger = Logger.getLogger(this.getClass)
  //模型对象dao
  val modelObjDao = new ModelObjDao

  /**
    * 1.访问hdfs路径读取模型个数
    * 2.创建模型对象
    * 3.清空原表中模型对象数据
    * 4.保存新的模型对象数据
    */
  def initialModelObj(): Unit = {
    //数据库连接
    var conn: Connection = null
    //保存模型对象的List
    var list = List[ModelObject]()

    try {
      //从数据库连接池中获取一个连接
      conn = ConnPoolUtil2.getConn()
      //设置非自动提交
      conn.setAutoCommit(false)
      //获取模型文件列表
      list = getModelFileList()
      if (list != null && list.size != 0) {
        //清空模型对象表，便于重新初始化
        modelObjDao.truncateModelObj(conn)
        conn.commit()
        //批量保存模型对象至mysql数据库
        modelObjDao.saveModelObjList2Mysql(list, conn)
        conn.commit()
        conn.setAutoCommit(true)
      }
    } catch {
      case e: SQLException => logger.error("SQL failure!!" + e.getMessage + e.printStackTrace())
      case e1: Exception => logger.error("Other Exception!!" + e1.getMessage + e1.printStackTrace())
    } finally {
      ConnPoolUtil2.releaseCon(conn)
    }
  }

  /**
    * 指标名称_指标周期_主机节点
    *
    * @return 模型结构对象
    */
  private def getModelFileList(): List[ModelObject] = {
    //保存所有模型对象的List
    var list = List[ModelObject]()
    //获取模型保存的HDFS根路径：/user/wsd/ml/
    val rootPath = props.getProperty("mlRootPath")
    //匹配 指标名称_指标周期 的正则表达式
    val indexRegex = props.getProperty("mlIndexNameRegex")
    //匹配 指标名称_指标周期
    val indexNameArrs = HDFSFileUtil.listFiles(rootPath).filter(_.matches(indexRegex))
    //遍历匹配到的 数组[指标名称_指标周期]
    for (indexNamePeriod <- indexNameArrs) {
      //指标名称
      val indexName = indexNamePeriod.substring(0, indexNamePeriod.lastIndexOf("_"))
      //周期
      val indexPeriod = indexNamePeriod.substring(indexNamePeriod.lastIndexOf("_") + 1)
      //数组[主机名]
      val hostArrs = HDFSFileUtil.listFiles(rootPath + System.getProperty("file.separator") + indexNamePeriod)
      //遍历 数组[主机名]
      for (host <- hostArrs) {
        //为每一个主机建立创建对应的模型对象ModelObject
        val modelObj = new ModelObject
        modelObj.setModelObjKey(indexNamePeriod + "_" + host)
        modelObj.setPredIndexName(indexName)
        modelObj.setPredPeriod(indexPeriod)
        modelObj.setHostName(host)
        modelObj.setPreCol("")
        modelObj.setIsValid("N")
        list = list.:+(modelObj)
      }
    }
    list
  }

  /**
    * 查询数据库获取模信息
    *
    * @return 模型信息list
    */
  def findModelObj(): util.List[ModelObject] = {
    var conn: Connection = null
    var list: util.List[ModelObject] = null
    try {
      conn = ConnPoolUtil2.getConn()
      list = modelObjDao.findModelObjAll(conn)
    } catch {
      case e: Exception => logger.error("SQL failure!!" + e.getMessage + e.printStackTrace())
      case e1: Exception => logger.error("Other Exception!!" + e1.getMessage + e1.printStackTrace())
    } finally {
      ConnPoolUtil2.releaseCon(conn)
    }
    list
  }
}
