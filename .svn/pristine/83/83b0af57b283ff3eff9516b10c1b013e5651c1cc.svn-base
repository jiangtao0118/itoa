package com.wisdom.spark.ml.model.gc

import com.wisdom.spark.common.util.SparkContextUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wisdom on 2017/2/28.
  */
object GCModeling {
  //日志类
  @transient
  val logger = Logger.getLogger(this.getClass)
  //HiveContext
  private var HiveCtx: HiveContext = null
  //目前仅训练gcmodel.gcType="scavenge"的数据，gcmodel.gcType="globle"的数据较少，按异常分类
  val gcType = "scavenge"
  /**
    * 训练高斯模型
    *
    * @param startTime      表名
    * @param hostName       主机名
    * @param endTime        gctype字段类型
    * @param corrLimit      是否保留字段的标准
    * @param k              独立高斯个数
    * @param convergenceTol 收敛标准
    * @param seed           种子
    * @param perTrunIterNum 迭代次数
    * @param modelSavePath  模型保存路径
    */
  def trainGMM(startTime: String, endTime: String, hostName: String, corrLimit: Double, k: Int, convergenceTol: Double,
               seed: Int, perTrunIterNum: Int, modelSavePath: String): Unit = {
    //注册的临时表表名
    val tableName = "gcmodel_tmp"
    //    读取Hive数据
    logger.warn("===================query data from registerTempTable======================")
    val parsedData = loadDataFromHive(tableName, 25, hostName, startTime, endTime)
    //    相关系数降维
    logger.warn("===================do corr================================================")
    val retainedData = drWithCORR(parsedData, corrLimit, hostName,modelSavePath)
    //    训练高斯混合模型
    logger.warn("===================训练高斯混合模型========================================")
    val gcGm = new GaussianMixture().setK(k)
      .setConvergenceTol(convergenceTol)
      .setSeed(seed)
      .setMaxIterations(perTrunIterNum)
    val gcGmm = gcGm.run(retainedData) //user/wsd/ml/zb_5/hostname/
    logger.warn("===================保存高斯混合模型=======================================")
    gcGmm.save(SparkContextUtil.getInstance(), modelSavePath + "/gc_" + gcType + "_0/" + hostName + "/gmm")
    logger.warn(printGMM(gcGmm))
  }

  /**
    * 打印高斯混合模型的信息
    *
    * @param gmm : 高斯混合模型
    */
  def printGMM(gmm: GaussianMixtureModel): Unit = {
    logger.warn("weights:")
    for (weight <- gmm.weights) {
      logger.warn(weight + "\n")
    }
    logger.warn("mus:")
    for (gaussian <- gmm.gaussians) {
      logger.warn(gaussian.mu.toString + "\n")
    }
    logger.warn("sigmas:")
    for (gaussian <- gmm.gaussians) {
      logger.warn(gaussian.sigma.toString() + "\n")
    }
  }

  /**
    * 使用相关系数降维
    *
    * @param parsedData    原始数据
    * @param corrLimit     是否保留字段的标准
    * @param hostName      主机名
    * @param modelSavePath 列索引保存路径
    * @return 降维后的新数据
    */
  def drWithCORR(parsedData: RDD[LabeledPoint], corrLimit: Double, hostName: String,modelSavePath: String): RDD[Vector] = {
    //需要分析的指标
    val labelDatas = parsedData.map { line => line.label }
    //需要保留的字段的对应的列的索引
    var retainedColumnIndexs = ArrayBuffer[Int]()
    //循环对41列计算相关系数
    for (i <- 0 to 40) {
      val columnDatas = parsedData.map(line => line.features(i))
      val columnCorr = Statistics.corr(labelDatas, columnDatas)
      println("================================================Statistics.corr:" + columnCorr)
      //如果相关系数不为NaN且绝对值大于给定的阈值，保留该列
      if (!columnCorr.isNaN && !columnCorr.isInfinity && columnCorr.abs > corrLimit) {
        println("corr:" + i + ":" + columnCorr)
        //将该列序号保存到retainedColumnIndexs中
        retainedColumnIndexs += i
      }
    }

    val path = new Path(modelSavePath + "/gc_" + gcType + "_0/" + hostName + "/mid/corr.csv")
    val conf = new Configuration()
    val fs = FileSystem.newInstance(conf)
    val os = fs.create(path)
    for (i <- retainedColumnIndexs.indices) {
      os.writeBytes(retainedColumnIndexs(i) + "\n")
    }
    os.close()

    parsedData.map { line => Vectors.dense(retainedColumnIndexs.map { index => line.features(index) }.toArray) }
  }

  /**
    * 从Hive读取数据
    *
    * @param tableName  :表名。
    * @param columnIdex ：目标指标的列数索引
    * @param hostName   ：主机名
    * @return :包含数据内容的rdd[LabeledPoint]
    */
  def loadDataFromHive(tableName: String, columnIdex: Int, hostName: String,startTime: String,
                       endTime: String): RDD[LabeledPoint] = {
    val hiveCtx = getHiveCtx()
    val filteredData = hiveCtx.sql(" SELECT * FROM " + tableName + " where gc_type='" + gcType +
      "' and hostname='" + hostName + "' and exclusiveStart_timestamp > '" + startTime +
      "' and exclusiveStart_timestamp < '" + endTime + "'")
    logger.warn("=============================" + hostName + " count: " + filteredData.count + "================================")
    filteredData.map { line =>
      LabeledPoint(line.getAs[Float](columnIdex).toDouble, Vectors.dense(Array(
        //line.getAs[Double](0),//exclusiveStart_timestamp,0
        line.getAs[Float](1).toDouble, //exclusiveStart_intervalms,1
        line.getAs[Long](2).toDouble, //afStart_totalBytesRequested,2
        //line.getAs[Double](3),//gc_type,3
        //line.getAs[Double](4),//gcStart_timestamp,4
        line.getAs[Long](5).toDouble, //gcStart_mem_free,5
        line.getAs[Long](6).toDouble, //gcStart_mem_total,6
        line.getAs[Long](7).toDouble, //gcStart_mem_percent,7
        line.getAs[Long](8).toDouble, //gcStart_nursery_free,8
        line.getAs[Long](9).toDouble, //gcStart_nursery_total,9
        line.getAs[Long](10).toDouble, //gcStart_nursery_percent,10
        line.getAs[Long](11).toDouble, //gcStart_tenure_free,11
        line.getAs[Long](12).toDouble, //gcStart_tenure_total,12
        line.getAs[Long](13).toDouble, //gcStart_tenure_percent,13
        line.getAs[Long](14).toDouble, //gcStart_soa_free,14
        line.getAs[Long](15).toDouble, //gcStart_soa_total,15
        line.getAs[Long](16).toDouble, //gcStart_soa_percent,16
        line.getAs[Long](17).toDouble, //gcStart_loa_free,17
        line.getAs[Long](18).toDouble, //gcStart_loa_total,18
        line.getAs[Long](19).toDouble, //gcStart_loa_percent,19
        line.getAs[Long](20).toDouble, //gcStart_rememberedSet_count,20
        line.getAs[Long](21).toDouble, //allocationStats_totalBytes,21
        line.getAs[Long](22).toDouble, //allocationStats_nonTlh,22
        line.getAs[Long](23).toDouble, //allocationStats_tlh,23
        line.getAs[Long](24).toDouble, //allocationStats_bytes,24
        line.getAs[Float](25).toDouble, //gcEnd_durationms,25
        //line.getAs[Double](26),//gcEnd_timestamp,26
        line.getAs[Long](27).toDouble, //gcEnd_mem_free,27
        line.getAs[Long](28).toDouble, //gcEnd_mem_total,28
        line.getAs[Long](29).toDouble, //gcEnd_mem_percent,29
        line.getAs[Long](30).toDouble, //gcEnd_nursery_free,30
        line.getAs[Long](31).toDouble, //gcEnd_nursery_total,31
        line.getAs[Long](32).toDouble, //gcEnd_nursery_percent,32
        line.getAs[Long](33).toDouble, //gcEnd_tenure_free,33
        line.getAs[Long](34).toDouble, //gcEnd_tenure_total,34
        line.getAs[Long](35).toDouble, //gcEnd_tenure_percent,35
        line.getAs[Long](36).toDouble, //gcEnd_soa_free,36
        line.getAs[Long](37).toDouble, //gcEnd_soa_total,37
        line.getAs[Long](38).toDouble, //gcEnd_soa_percent,38
        line.getAs[Long](38).toDouble, //gcEnd_loa_free,39
        line.getAs[Long](40).toDouble, //gcEnd_loa_total,40
        line.getAs[Long](41).toDouble, //gcEnd_loa_percent,41
        line.getAs[Long](42).toDouble, //gcEnd_rememberedSet_count,42
        line.getAs[Long](43).toDouble, //bytesRequested,43
        //line.getAs[Double](44),//exclusiveEnd_timestamp,44
        line.getAs[Float](45).toDouble //exclusiveEnd_durationms,45
        //line.getAs[Double](46),//hostName,46
      )))
    }
  }

  /**
    * 查询所有主机
    *
    * @return
    */
  def queryGCHostName(): Array[Row] = {
    val hiveCtx = getHiveCtx()
    val t1 = System.currentTimeMillis()
    val allData = hiveCtx.sql(" SELECT hostname FROM gc.gcmodel group by hostname")
    val t2 = System.currentTimeMillis()
    logger.warn("****  查询所有主机处理耗时: ===> " + (t2 - t1) + " ms")
    allData.collect()
  }

  /**
    * 获取HiveContext实例
    *
    * @return HiveContext
    */
  def getHiveCtx(): HiveContext = {
    if (HiveCtx == null) {
      HiveCtx = new HiveContext(SparkContextUtil.getInstance())
      HiveCtx
    }
    else
      HiveCtx
  }


  /**
    * GC数据建模程序入口
    *
    * @param args :
    *             args(0):GC数据表名
    *             args(1):gc type
    *             args(2):保留字段的最小相关系数
    *             args(3):gmm独立高斯个数
    *             args(4):gmm收敛标准
    *             args(5):gmm种子
    *             args(6):gmm迭代次数
    *             args(7):模型保存路径
    *
    */
  def main(args: Array[String]) {
    if (args.length != 8) {
      println("need 8 args,like: \"startTime endTime corrLimit k convergenceTol seed perTrunIterNum modelSavePath\"\n")
      System.exit(1)
    }
    //获取SparkContext
    val sc = SparkContextUtil.getInstance()
    //设置训练数据的开始时间
    val startTime = args(0)
    //设置训练数据的结束时间
    val endTime = args(1)
    //保留字段的最小相关系数
    val corrLimit = args(2).toDouble
    //gmm独立高斯个数
    val k = args(3).toInt
    //gmm收敛标准
    val convergenceTol = args(4).toDouble
    //种子
    val seed = args(5).toInt
    //gmm迭代次数
    val perTrunIterNum = args(6).toInt
    //设置模型保存路径
    val modelSavePath = args(7)
    // 查询所有主机
    val hostNames = queryGCHostName()
    // 获取HiveContext
    val hiveCtx = getHiveCtx()
    //设置临时表名称
    val tableName = "gc.gcmodel"
    //查询训练数据（包括全部主机、所有历史数据、所有字段非空）
    val gcDF = hiveCtx.sql(" SELECT * FROM " + tableName + " where gc_type='" + gcType +
      "' and exclusiveStart_intervalms is not NULL "
      + "and afStart_totalBytesRequested is not NULL "
      + "and gcStart_mem_free is not NULL "
      + "and gcStart_mem_total is not NULL "
      + "and gcStart_mem_percent is not NULL "
      + "and gcStart_nursery_free is not NULL "
      + "and gcStart_nursery_total is not NULL "
      + "and gcStart_nursery_percent is not NULL "
      + "and gcStart_tenure_free is not NULL "
      + "and gcStart_tenure_total is not NULL "
      + "and gcStart_tenure_percent is not NULL "
      + "and gcStart_soa_free is not NULL "
      + "and gcStart_soa_total is not NULL "
      + "and gcStart_soa_percent is not NULL "
      + "and gcStart_loa_free is not NULL "
      + "and gcStart_loa_total is not NULL "
      + "and gcStart_loa_percent is not NULL "
      + "and gcStart_rememberedSet_count is not NULL "
      + "and allocationStats_totalBytes is not NULL "
      + "and allocationStats_nonTlh is not NULL "
      + "and allocationStats_tlh is not NULL "
      + "and allocationStats_bytes is not NULL "
      + "and gcEnd_durationms is not NULL "
      + "and gcEnd_mem_free is not NULL "
      + "and gcEnd_mem_total is not NULL "
      + "and gcEnd_mem_percent is not NULL "
      + "and gcEnd_nursery_free is not NULL "
      + "and gcEnd_nursery_total is not NULL "
      + "and gcEnd_nursery_percent is not NULL "
      + "and gcEnd_tenure_free is not NULL "
      + "and gcEnd_tenure_total is not NULL "
      + "and gcEnd_tenure_percent is not NULL "
      + "and gcEnd_soa_free is not NULL "
      + "and gcEnd_soa_total is not NULL "
      + "and gcEnd_soa_percent is not NULL "
      + "and gcEnd_loa_free is not NULL "
      + "and gcEnd_loa_total is not NULL "
      + "and gcEnd_loa_percent is not NULL "
      + "and gcEnd_rememberedSet_count is not NULL "
      + "and bytesRequested is not NULL "
      + "and exclusiveEnd_durationms is not NULL "
      + "and length(trim(exclusiveStart_intervalms)) > 0 "
      + "and length(trim(afStart_totalBytesRequested)) > 0 "
      + "and length(trim(gcStart_mem_free)) > 0 "
      + "and length(trim(gcStart_mem_total)) > 0 "
      + "and length(trim(gcStart_mem_percent)) > 0 "
      + "and length(trim(gcStart_nursery_free)) > 0 "
      + "and length(trim(gcStart_nursery_total)) > 0 "
      + "and length(trim(gcStart_nursery_percent)) > 0 "
      + "and length(trim(gcStart_tenure_free)) > 0 "
      + "and length(trim(gcStart_tenure_total)) > 0 "
      + "and length(trim(gcStart_tenure_percent)) > 0 "
      + "and length(trim(gcStart_soa_free)) > 0 "
      + "and length(trim(gcStart_soa_total)) > 0 "
      + "and length(trim(gcStart_soa_percent)) > 0 "
      + "and length(trim(gcStart_loa_free)) > 0 "
      + "and length(trim(gcStart_loa_total)) > 0 "
      + "and length(trim(gcStart_loa_percent)) > 0 "
      + "and length(trim(gcStart_rememberedSet_count)) > 0 "
      + "and length(trim(allocationStats_totalBytes)) > 0 "
      + "and length(trim(allocationStats_nonTlh)) > 0 "
      + "and length(trim(allocationStats_tlh)) > 0 "
      + "and length(trim(allocationStats_bytes)) > 0 "
      + "and length(trim(gcEnd_durationms)) > 0 "
      + "and length(trim(gcEnd_mem_free)) > 0 "
      + "and length(trim(gcEnd_mem_total)) > 0 "
      + "and length(trim(gcEnd_mem_percent)) > 0 "
      + "and length(trim(gcEnd_nursery_free)) > 0 "
      + "and length(trim(gcEnd_nursery_total)) > 0 "
      + "and length(trim(gcEnd_nursery_percent)) > 0 "
      + "and length(trim(gcEnd_tenure_free)) > 0 "
      + "and length(trim(gcEnd_tenure_total)) > 0 "
      + "and length(trim(gcEnd_tenure_percent)) > 0 "
      + "and length(trim(gcEnd_soa_free)) > 0 "
      + "and length(trim(gcEnd_soa_total)) > 0 "
      + "and length(trim(gcEnd_soa_percent)) > 0 "
      + "and length(trim(gcEnd_loa_free)) > 0 "
      + "and length(trim(gcEnd_loa_total)) > 0 "
      + "and length(trim(gcEnd_loa_percent)) > 0 "
      + "and length(trim(gcEnd_rememberedSet_count)) > 0 "
      + "and length(trim(bytesRequested)) > 0 "
      + "and length(trim(exclusiveEnd_durationms)) > 0 "
      + "order by gcStart_timestamp asc"
    )
    //注册临时表
    gcDF.registerTempTable("gcmodel_tmp")
    //打印临时表中的数据量
    logger.warn("===================================gcmodel.count="+gcDF.count()+"========================")
    //    为所有主机训练模型
    for (hostName <- hostNames) {
      //调用训练模型方法
      trainGMM(startTime, endTime, hostName(0).toString, corrLimit, k, convergenceTol, seed, perTrunIterNum, modelSavePath)
    }
    //训练结束，退出程序
    sc.stop()
  }
}
