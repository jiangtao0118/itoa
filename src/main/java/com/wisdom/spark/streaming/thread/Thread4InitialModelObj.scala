package com.wisdom.spark.streaming.thread

import java.sql.PreparedStatement
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.ml.mlUtil.PropertyUtil
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import com.wisdom.spark.streaming.bean.ModelObject
import com.wisdom.spark.streaming.service.ModelObjService
import org.apache.calcite.plan.RelOptPlanner.Executor
import com.wisdom.spark.streaming.tools.{ModelInstantiationThread, ModelInstantiationThread2}
import org.apache.log4j.Logger

import scala.collection.mutable.HashMap

/**
  * Created by zhengz on 2017/1/19.
  */
object Thread4InitialModelObj extends Serializable {
  val logger = Logger.getLogger(this.getClass)

  //系统第一次部署，初始化模型列表到数据库
  def main(args: Array[String]) {
    val modelObjService = new ModelObjService
    logger.info("----------- Initial Model Object ------------")
//    modelObjService.initialModelObj()
    getModelMap
    logger.info("----------- END ------------")
  }

  /**
    * (指标_预测周期_节点,AllPredcictTarget对象)
    * 指标_预测周期_节点: 例如IDEL_CPU_5_ASCECUP01
    */

  val tgtModelMap = new ConcurrentHashMap[String, AllPredcictTarget]
  //  synchronized(tgtModelMap)
  var objNums: Integer = 0


  /**
    * 初始化模型对象，加入map缓存中
    *
    * @return
    */
  private def tgtModelMapFromDB(): Unit = {
    val props = ItoaPropertyUtil.getProperties()
    val maxThreadNums = props.getProperty("spark.driver.thread.max").toInt
    val modelObjService = new ModelObjService
    val listModelObj: util.List[ModelObject] = modelObjService.findModelObj()
    objNums = listModelObj.size()
    val pools = Executors.newFixedThreadPool(maxThreadNums) //创建线程池

    for (i <- 0 until listModelObj.size()) {
      val modelObj = listModelObj.get(i)
      pools.execute(new ModelInstantiationThread(modelObj.getModelObjKey, modelObj.getPredIndexName + "_" + modelObj.getPredPeriod, modelObj.getHostName))
    }
  }

  case class ModelRecord(
                         tgtvar: String,
                         hostnode: String,
                         period: String,
                         model_typ : String ,
                         flag : String,
                         recordTime: String,
                         reserveCol1: String,
                         reserveCol2: String
                       )
  def queryData(): util.ArrayList[ModelRecord] = {
    val conn=ConnPoolUtil2.getConn()
    //新建List，将结果保存在List里面
    var list: util.ArrayList[ModelRecord] = null
    var pst: PreparedStatement = null
    //创建查询语句
//    val sql = "select * from t_index_alarm_type where flag='01' and model_typ in ('PPN','WINSTATS')"
    val sql=PropertyUtil.getProperty("model.init.sql")
    pst = conn.prepareStatement(sql)
    //执行查询
    val res = pst.executeQuery()
    //将list赋值给包含DataRecord类型的数据
    list = new util.ArrayList[ModelRecord]()
    while (res.next()) {
      val datarecord = ModelRecord(res.getString(1), res.getString(2), res.getString(3), res.getString(4), res.getString(5), res.getString(6), res.getString(7), res.getString(8))
      list.add(datarecord)
    }
    ConnPoolUtil2.releaseCon(conn)
    list
  }

  private def tgtModelMapFromDB2(): Unit = {
    val props = ItoaPropertyUtil.getProperties()
    val maxThreadNums = props.getProperty("spark.driver.thread.max").toInt
    val listModelObj: util.ArrayList[ModelRecord] = queryData()
    objNums = listModelObj.size()
    val pools = Executors.newFixedThreadPool(maxThreadNums) //创建线程池

    for (i <- 0 until listModelObj.size()) {
      val modelObj = listModelObj.get(i)
      pools.execute(new ModelInstantiationThread2(modelObj))
    }
  }

  //程序每次启动时，预加载模型缓存早内存中
  def getModelMap(): ConcurrentHashMap[String, AllPredcictTarget] = {
    val time1 = System.currentTimeMillis()
//    tgtModelMapFromDB()
    tgtModelMapFromDB2()

    while (tgtModelMap.size() != objNums) {
      logger.warn("**** tgtModelMap.size:" + tgtModelMap.size + " == objNums:" + objNums + " ??  ****")
      logger.warn("**** Waiting for model objects instantiated1111... ****")
      Thread.sleep(10000)
    }
    logger.warn("**** tgtModelMap.size:" + tgtModelMap.size + " == objNums:" + objNums + " !!  ****")

    val time2 = System.currentTimeMillis()
    logger.warn("------------------ All Model objects instantiated cost：" + (time2 - time1) + "ms")
    tgtModelMap
  }
}

