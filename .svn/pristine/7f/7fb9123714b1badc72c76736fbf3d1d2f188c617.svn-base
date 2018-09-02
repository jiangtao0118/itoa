package com.wisdom.spark.ml.tgtVar

import java.text.DecimalFormat
import java.util

import com.wisdom.spark.common.log.WSDLog
import com.wisdom.spark.ml.db.GMMTrainParamDao
import com.wisdom.spark.ml.features.{CorrCovExt, MLlibPCA, StandardScalerExt}
import com.wisdom.spark.ml.model.pnn.MLlibGaussianMixture
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import com.wisdom.spark.ml.mlUtil._
import org.apache.commons.math3.util.MathUtils
import org.apache.spark.mllib.stat.Statistics


object GMMPredictor {

  val logger = ContextUtil.logger
  val usage =
    """
Usage:spark-submit --master yarn-client [spark options] --class GMMPredictorWithPCA JSparkML.jar <command args>

<command args> :
  checkModel targetVar hostnode inPath preOutPath method 测试GMM精度
      |targetVar 预测指标
      |hostnode 主机节点
      |inPath 整合后的原始数据
      |preOutPath 预测输出数据
      |method CORR 或 PCA
  trainModel targetVar hostnode tolerance k maxIter method seed betaMin betaStep betaMax训练高斯混合模型
      |targetVar 预测指标
      |hostnode 主机节点
      |tolerance 收敛精度
      |k 聚类簇数
      |maxIter 最大迭代次数
      |method CORR 或 PCA
      |seed 随机种子
      |betaMin DAEM beta下限
      |betaStep DAEM
      |betaMax DAEM
  sensitivityAnalysis targetVar hostnode inPath preOutPath sen_rate sen_col 测试GMM精度
      |targetVar 预测指标
      |hostnode 主机节点
      |inPath 整合后的原始数据
      |preOutPath 预测输出数据
      |sen_rate 敏感度调整率，0到1之间的数
      |sen_col 字段序号，从0开始，如果无字段序号则对所有字段分别做单因素敏感性分析
  autoTrainModel trainFile testFile targetVar hostnode corrThreshold corrStep tolerance k maxIter seed betaMin betaStep betaMax randomCnt maerThreshold maxDmCnt 训练高斯混合模型
      |trainFile        训练文件
      |testFile         测试文件
      |targetVar        预测指标
      |hostnode         主机节点
      |corrThreshold    降维阈值           小数   [0-1]
      |corrStep         阈值降低步幅       小数   (0-1)
      |tolerance        收敛精度           小数   (0-1)
      |kMin             聚类簇数-最小值    正整数 [2,正无穷)
      |kStep            聚类簇数-迭代步长  正整数 [2,正无穷)
      |kMax             聚类簇数-最大值    正整数 [kMin,正无穷) 要大于kMin
      |maxIter          最大迭代次数       正整数
      |seed             随机种子           正整数
      |betaMin          DAEM beta下限      小数   (0-1],一般取1
      |betaStep         DAEM 迭代步长      小数   [0-1),一般取0
      |betaMax          DAEM beta上限      小数   [1-2),一般取1
      |randomCnt        随机次数           正整数 [1,正无穷) 一般取2或3
      |maerThreshold    平均绝对误差率阈值 小数   (0-1)
      |maxTrainCnt      最大训练模型数     正整数
    """

  def main(args: Array[String]): Unit = {

    val sc = ContextUtil.sc

    if (args.length == 0) {
      help()
    }

    val method = args(0)
    if (method.equalsIgnoreCase("checkModel")) {

      if (args.length != 6) {
        help()
      }

      val tgt = args(1)
      val hostnode = args(2)
      val inPath = args(3)
      val preOutPath = args(4)
      val dimRed = args(5)

      HDFSFileUtil.deleteFile(preOutPath)

      val fdata = DataFrameUtil.readCsv(inPath)
      val fdataRDD = DataFrameUtil.dfToRDDofArray(fdata)

      fdataRDD.cache()

      if (dimRed == "PCA") {
        val cpuGMM = new GMMPredictorWithPCA(tgt, hostnode)
        val out = cpuGMM.checkModel(fdataRDD)
        out.saveAsTextFile(preOutPath)
      }
      else if (dimRed == "CORR") {

        logger.info("开始校验模型==============>>" + tgt + "," + hostnode)

        val stTime = System.currentTimeMillis()

        val cpuGMM = new GMMPredictorWithCorr(tgt, hostnode)

        val out = cpuGMM.checkModel(fdataRDD).cache()
        out.saveAsTextFile(preOutPath)

        val endTime = System.currentTimeMillis()

        val outArr = out.map(_.split(",")).map(x => (x(1).toDouble, x(2).toDouble)).cache()
        val outArrActual = outArr.map(x => x._1)
        val outArrPre = outArr.map(x => x._2)

        val cnt = outArr.count()
        val costTime = (endTime - stTime) / 1000
        val corr = Statistics.corr(outArrActual, outArrPre)
        //平均绝对误差
        val mae = outArr.map(x => math.abs(x._1 - x._2)).sum() / cnt
        //平均绝对误差率
        val maer = outArr.map(x => math.abs(x._1 - x._2) / math.abs(x._1)).sum() / cnt
        //误差平方和
        val sse = outArr.map(x => math.pow((x._1 - x._2), 2)).sum()

        logger.info("GMM testing parameter=================>" + tgt + "," + hostnode + "," + cnt + "," + costTime + "," + corr + "," + mae + "," + maer + "," + sse)

        logger.info("完成校验模型==============>>" + tgt + "," + hostnode)

      }
      else {
        WSDLog.getLogger(this).info("Not Suport Method:" + dimRed)
      }

    }
    else if (method.equalsIgnoreCase("sensitivityAnalysis")) {

      if (args.length < 6) {
        help()
      }

      val tgt = args(1)
      val hostnode = args(2)
      val inPath = args(3)
      val preOutPath = args(4)
      val sen_rate = args(5).toDouble


      HDFSFileUtil.deleteFile(preOutPath)

      val fdata = DataFrameUtil.readCsv(inPath)
      val fdataRDD = DataFrameUtil.dfToRDDofArray(fdata)

      fdataRDD.cache()

      val cpuGMM = new GMMPredictorWithCorr(tgt, hostnode)

      if (args.length == 7) {
        val sen_col = args(6).toInt
        val out = cpuGMM.sensitivityAnalysis(fdataRDD, sen_col, sen_rate)
        out.saveAsTextFile(preOutPath)
      }
      else {

        val out = cpuGMM.sensitivityAnalysis(fdataRDD, sen_rate)
        out.saveAsTextFile(preOutPath)

      }

    }
    else if (method.equalsIgnoreCase("trainModel")) {
      if (args.length != 11) {
        help()
      }
      val tgt = args(1)
      val hostnode = args(2)
      val tolerance = args(3).toDouble
      val k = args(4).toInt
      val maxIter = args(5).toInt
      val dimRed = args(6)
      val seed = args(7).toLong
      val betaMin = args(8).toDouble
      val betaStep = args(9).toDouble
      val betaMax = args(10).toDouble

      val gm = new MLlibGaussianMixture()

      var inPath = ""
      if (dimRed == "CORR") {
        inPath = MLPathUtil.corrDataPath(tgt, hostnode) //关系矩阵方法降维后的数据
      }
      else if (dimRed == "PCA") {
        inPath = MLPathUtil.pcaDataPath(tgt, hostnode) //PCA方法映射降维后的数据
      }
      else {
        WSDLog.getLogger(this).info("Not Suport Method:" + dimRed)
      }

      logger.info("开始训练模型==============>>" + tgt + "," + hostnode)

      val modelPath = MLPathUtil.gmmModelPath(tgt, hostnode)

      val model = gm.trainModel(inPath, modelPath, tolerance, k, maxIter, seed, betaMin, betaStep, betaMax)

      for (i <- 0 until model.weights.length) {
        logger.info("Gaussian_" + i + " weights:" + model.weights(i) + " mu:" + model.gaussians(i).mu.toArray.mkString(","))
      }

      logger.info("结束训练模型==============>>" + tgt + "," + hostnode)

    }
    else if (method.equalsIgnoreCase("autoTrainModel")) {

      if (args.length < 19) {
        help()
      }

      val trainFile = args(1)
      val testFile = args(2)
      val tgtVar = args(3)
      val hostnode = args(4)
      val corrThreshold = args(5).toDouble
      val corrStep = args(6).toDouble
      val tolerance = args(7).toDouble
      val clustersMin = args(8).toInt
      val clustersStep = args(9).toInt
      val clustersMax = args(10).toInt
      val maxIter = args(11).toInt
      val seed = args(12).toLong
      val betaMin = args(13).toDouble
      val betaStep = args(14).toDouble
      val betaMax = args(15).toDouble
      val randomCnt = args(16).toInt
      val maerThreshold = args(17).toDouble
      val maxTrainCnt = args(18).toInt

      val list = GMMPredictor.autoCorrClustering(
        trainFile,
        testFile,
        tgtVar,
        hostnode,
        corrThreshold,
        corrStep,
        clustersMin,
        clustersStep,
        clustersMax,
        maxIter,
        tolerance,
        seed,
        betaMin,
        betaStep,
        betaMax,
        randomCnt,
        maerThreshold,
        maxTrainCnt
      )

      var unchange = true

      for (i <- 0 until list.size()) {
        val params = list.get(i)
        if (params.updatedModel) {
          unchange = false
        }
      }
      if (unchange) {
        logger.info(tgtVar + "," + hostnode + ": all MAER are greater than maerThreshold. 没有找到满足误差率阈值的模型")
      }
      logger.info(list)

      val modelObjDao = new GMMTrainParamDao
      modelObjDao.saveTrainParamList2Mysql(list)

    }
    else {
      help()
    }

    ContextUtil.sc.stop()

  }

  def help(): Unit = {
    println(usage)
    System.exit(1)
  }

  def autoCorrClustering(trainFile: String,
                         testFile: String,
                         tgtVar: String,
                         hostnode: String,
                         corrThreshold: Double,
                         corrStep: Double,
                         clustersMin: Int,
                         clustersStep: Int,
                         clustersMax: Int,
                         maxIter: Int,
                         tolerance: Double,
                         seed: Long,
                         betaMin: Double,
                         betaStep: Double,
                         betaMax: Double,
                         randomCnt: Int,
                         maerThreshold: Double,
                         maxTrainCnt: Int
                        ): util.ArrayList[GMMTrainParams] = {

    val trainList = new util.ArrayList[GMMTrainParams]()
    var corr = corrThreshold
    var lasttraincnt = 0
    var lastSSE = Double.MaxValue
    var unchange = true

    val fdata = DataFrameUtil.readCsv(trainFile)
    val data = DataFrameUtil.dfToRDDofArray(fdata).map(x => Vectors.dense(x))
    //计算相关系数矩阵
    val corrMat = Statistics.corr(data, "pearson")
    val cnt = corrMat.numCols - 1
    val corrArr = new Array[Double](cnt)
    //去除最后一个自相关系数1
    for (i <- 0 until cnt) {
      //排除Double.NaN
      if (math.abs(corrMat(i, cnt)) >= 0) {
        corrArr(i)=math.abs(corrMat(i, cnt))
      }
      else {
        corrArr(i)=math.abs(0)
      }

    }
    val sortedCorrArr = corrArr.sortBy(x => -x)

    var i=0
    do {
      if (corr > sortedCorrArr(i)) {
        corr = sortedCorrArr(i)
        logger.info("第" + i + "次指定相关系数阈值" + corrThreshold + "大于训练数据中与目标变量的相关系数第" + i + "个值" + sortedCorrArr(i) + ", 将更改为此值。")
      }

      logger.info("训练模型，降维阈值：" + corr)
      val tmpList = autoClustering(
        trainFile,
        testFile,
        tgtVar,
        hostnode,
        corr, //改变相关系数阈值
        clustersMin,
        clustersStep,
        clustersMax,
        maxIter,
        tolerance,
        seed,
        betaMin,
        betaStep,
        betaMax,
        randomCnt,
        maerThreshold,
        lasttraincnt, //更新模型训练数量
        maxTrainCnt,
        lastSSE
      )
      for (i <- 0 until tmpList.size()) {
        val params = tmpList.get(i)
        if (params.updatedModel) {
          unchange = false
        }

        lastSSE = params.testParam.testSSE
      }

      trainList.addAll(tmpList)
      lasttraincnt += tmpList.size()

      corr -= corrStep
      i = i+1
    } while (
      corr > 0
        && corrStep > 0
        //        && unchange
        && lasttraincnt < maxTrainCnt
    )

    trainList

  }

  def autoClustering(trainFile: String,
                     testFile: String,
                     tgtVar: String,
                     hostnode: String,
                     corrThreshold: Double,
                     clustersMin: Int,
                     clustersStep: Int,
                     clustersMax: Int,
                     maxIter: Int,
                     tolerance: Double,
                     seed: Long,
                     betaMin: Double,
                     betaStep: Double,
                     betaMax: Double,
                     randomCnt: Int,
                     maerThreshold: Double,
                     lasttraincnt: Int,
                     maxTrainCnt: Int,
                     lastSSE: Double
                    ): util.ArrayList[GMMTrainParams] = {

    val trainList = new util.ArrayList[GMMTrainParams]()
    var clusters = clustersMin
    val clusterStep = clustersStep
    val GMMModelPath = MLPathUtil.gmmModelPath(tgtVar, hostnode)
    var unchange = true
    var lasttraincnt_0 = lasttraincnt
    var lastSSE_0 = lastSSE

    do {
      logger.info("训练模型，Cluster个数：" + clusters)
      val tmpList = autoTraining(
        trainFile,
        testFile,
        tgtVar,
        hostnode,
        corrThreshold,
        clusters, //提升模型复杂度
        maxIter,
        tolerance,
        seed,
        betaMin,
        betaStep,
        betaMax,
        randomCnt,
        maerThreshold,
        lasttraincnt_0, //更新模型训练数量
        maxTrainCnt,
        lastSSE_0
      )

      for (i <- 0 until tmpList.size()) {
        val params = tmpList.get(i)
        if (params.updatedModel) {
          unchange = false
          //          lastdmcnt = params.trainParam.traindmCnt
        }
        lastSSE_0 = params.testParam.testSSE
      }

      trainList.addAll(tmpList)
      lasttraincnt_0 += tmpList.size()
      clusters += clusterStep
    } while (clusters <= clustersMax
      //      && unchange
      && lasttraincnt_0 < maxTrainCnt
    )

    trainList
  }

  case class GMMTrainParams(
                             tgtVar: String,
                             hostnode: String,
                             trainParam: GMMTrainPart,
                             testParam: GMMTestPart,
                             updatedModel: Boolean
                           )

  case class GMMTrainPart(
                           trainFile: String,
                           testFile: String,
                           corrThreshold: Double,
                           corrCols: Array[String],
                           clusters: Int,
                           maxIter: Int,
                           tolerance: Double,
                           seed: Long,
                           betaMin: Double,
                           betaStep: Double,
                           betaMax: Double,
                           randomCnt: Int,

                           trainRecordCnt: Long,
                           trainCorrCost: Long, //
                           traindmCnt: Int,
                           trainCost: Long //
                         )

  case class GMMTestPart(
                          testRecordCnt: Long,
                          testCost: Long,
                          testCorr: Double,
                          testMAE: Double,
                          testMAER: Double,
                          testSSE: Double,
                          preOutPath: String
                        )

  def autoTraining(trainFile: String,
                   testFile: String,
                   tgtVar: String,
                   hostnode: String,
                   corrThreshold: Double,
                   clusters: Int,
                   maxIter: Int,
                   tolerance: Double,
                   seed: Long,
                   betaMin: Double,
                   betaStep: Double,
                   betaMax: Double,
                   randomCnt: Int,
                   maerThreshold: Double,
                   lasttraincnt: Int,
                   maxTrainCnt: Int,
                   lastSSE_0: Double
                  ): util.ArrayList[GMMTrainParams] = {
    var updatedModel = false
    var lasttraincnt_0 = lasttraincnt
    var lastSSE = lastSSE_0
    val trainParaList = new util.ArrayList[GMMTrainParams]()

    if (lasttraincnt_0 < maxTrainCnt) {
      val GMMModelPath = MLPathUtil.gmmModelPath(tgtVar, hostnode)
      val GMMModelTmpPath = MLPathUtil.mlRootPath + "/" + tgtVar + "/" + hostnode + "_tmp"
      val mvsrc = MLPathUtil.mlRootPath + "/" + tgtVar + "/" + hostnode
      HDFSFileUtil.deleteFile(GMMModelTmpPath)
      if (HDFSFileUtil.exists(mvsrc)) {
        HDFSFileUtil.moveFile(mvsrc, GMMModelTmpPath)
      }

      val scalerFile = MLPathUtil.scalerDataPath(tgtVar, hostnode)
      val corrFile = MLPathUtil.corrDataPath(tgtVar, hostnode)
      val corrExt = new CorrCovExt()

      val scaler = new StandardScalerExt()
      val scalermodelPath = MLPathUtil.scalerModelPath(tgtVar, hostnode)

      val leftCnt = math.min(randomCnt, maxTrainCnt - lasttraincnt_0)
      for (j <- 0 until leftCnt) {

        scaler.transformAndSave(trainFile,
          scalerFile,
          scalermodelPath)

        val stcorrtime = System.currentTimeMillis()
        val tgt_ind = tgtVar.substring(0, tgtVar.lastIndexOf("_"))
        val whitelistRegx = PropertyUtil.getProperty("ml.whitelist." + tgt_ind)
        val corrout = corrExt.corrAndSave("pearson", corrThreshold, scalerFile, corrFile, whitelistRegx)
        val edcorrtime = System.currentTimeMillis()
        val trainCorrCost = edcorrtime - stcorrtime
        val rdcnt = corrout._2.length
        logger.info("降维耗时：" + trainCorrCost)
        logger.info("剩余维度：" + rdcnt)

        val sttraintime = System.currentTimeMillis()
        val gm = new MLlibGaussianMixture()
        gm.trainModel(corrFile, GMMModelPath, tolerance, clusters, maxIter, seed, betaMin, betaStep, betaMax)
        val edtraintime = System.currentTimeMillis()
        val trainCost = edtraintime - sttraintime

        val trainRecordCnt = DataFrameUtil.readCsv(corrFile).count()

        val preOutPath = MLPathUtil.mlRootPath + "/testOut/" + tgtVar + "/" + hostnode + "/" + System.currentTimeMillis() + "/"
        HDFSFileUtil.deleteFile(preOutPath)
        val fdata = DataFrameUtil.readCsv(testFile)
        val fdataRDD = DataFrameUtil.dfToRDDofArray(fdata)
        fdataRDD.cache()
        val stTime = System.currentTimeMillis()
        val cpuGMM = new GMMPredictorWithCorr(tgtVar, hostnode)
        val checkout = cpuGMM.checkModel(fdataRDD)
        checkout.saveAsTextFile(preOutPath)
        val endTime = System.currentTimeMillis()
        val outArr = checkout.map(_.split(",")).map(x => (x(1).toDouble, x(2).toDouble)).cache()
        val outArrActual = outArr.map(x => x._1)
        val outArrPre = outArr.map(x => x._2)
        val cnt = outArr.count()
        val costTime = (endTime - stTime)
        var corr = Statistics.corr(outArrActual, outArrPre)
        if(corr.isNaN){
          corr = -1.0
        }
        //平均绝对误差
        val mae = outArr.map(x => math.abs(x._1 - x._2)).sum() / cnt
        //平均绝对误差率

        val maer = outArr.map(x => {
          val epsion = 1e-7
          if (math.abs(x._1) < epsion) {
            math.abs(x._1 - x._2)
          } else {
            math.abs(x._1 - x._2) / math.abs(x._1)
          }
        }).sum() / cnt
        //误差平方和
        val sse = outArr.map(x => math.pow((x._1 - x._2), 2)).sum()

        logger.info("MAER: " + maer + ", SSE: " + sse)
        if (maer <= maerThreshold && sse < lastSSE) {
          logger.info("精度提升，更新模型")
          HDFSFileUtil.deleteFile(GMMModelTmpPath)
          HDFSFileUtil.moveFile(mvsrc, GMMModelTmpPath)
          updatedModel = true
          lastSSE = sse
        }
        else {
          HDFSFileUtil.deleteFile(GMMModelPath)
        }

        val trainparam = GMMTrainPart(
          trainFile,
          testFile,
          corrThreshold,
          corrout._2,
          clusters,
          maxIter,
          tolerance,
          seed,
          betaMin,
          betaStep,
          betaMax,
          randomCnt,

          trainRecordCnt,
          trainCorrCost,
          rdcnt,
          trainCost
        )

        val testparam = GMMTestPart(
          cnt,
          costTime,
          corr,
          mae,
          maer,
          sse,
          preOutPath
        )

        trainParaList.add(GMMTrainParams(
          tgtVar,
          hostnode,
          trainparam,
          testparam,
          updatedModel
        ))

        lasttraincnt_0 += 1
        updatedModel = false

      }

      if (HDFSFileUtil.exists(GMMModelTmpPath)) {
        HDFSFileUtil.deleteFile(mvsrc)
        HDFSFileUtil.moveFile(GMMModelTmpPath, mvsrc)
      }
    }
    trainParaList
  }

}

/**
  * Created by wisdom on 2016/11/16.
  * 根据原始数据进行预测
  * 原始数据格式与训练模型采用数据应一致
  * 步骤：
  * 首先对原始数据进行标准化，尺度使用训练模型产生的scalerModelPath
  * 再对标准化后的数据降维，降维方法支持：关系矩阵方法-CORR，PCA方法-PCA
  * 基于训练模型产生的关系矩阵相关字段corrDataPath，降维时需要输入数据的字段名，要求与训练数据一致
  * 最后调用训练出的GMM模型预测gmmModelPath
  */
class GMMPredictorWithPCA(tgt: String,
                          hostnode: String) extends Serializable {

  //价值gmm模型
  val gmmModelPath = MLPathUtil.gmmModelPath(tgt, hostnode)
  val gmm = new MLlibGaussianMixture()
  val jgmm = gmm.getGMM(gmmModelPath)

  //加载标准化模型
  val stdScalerExt = new StandardScalerExt()
  val scalerModelPath = MLPathUtil.scalerModelPath(tgt, hostnode)
  val scalerModel = stdScalerExt.getModel(scalerModelPath)
  val mean = scalerModel.mean.toArray
  val std = scalerModel.std.toArray

  //  //关系矩阵法筛选的变量
  //  val corrDataCols = PropertyUtil.getModelProperty(MLPathUtil.corrFeatureColsKey(tgt,hostnode)).split(",")
  //  val columns = corrDataCols
  //
  //  //所有输入变量名及关系矩阵法筛选的变量对应索引，featureCols包含了最后一列的待预测字段名称
  //  val featureCols = PropertyUtil.getModelProperty(MLPathUtil.featureColsKey(tgt,hostnode)).split(",")
  //  val colIndex = new Array[Int](columns.length)
  //  for(i <- 0 to columns.length-1){
  //    colIndex(i)=featureCols.indexOf(columns(i))
  //  }

  //取得PCA的PC矩阵
  val pcaModel = new MLlibPCA()
  val pcaModelPath = MLPathUtil.pcaModelPath(tgt, hostnode)
  val pcaMat = pcaModel.getPCMatrix(pcaModelPath)

  /**
    * 针对单条数据预测
    *
    * @param data 输入原始features数据，要求与训练数据结构一致
    * @return 英文逗号分隔数组，预测值 下界 上界
    */
  def predict(data: Array[Double]): Array[Double] = {

    val in = data
    val scalerArr = getScaler(in)
    //    val reduceDim = pcaModel.transformAndScaler(scalerArr,pcaMat,pcaModelPath)
    val reduceDim = pcaModel.transform(scalerArr, pcaMat)
    //    println("SCALER:"+scalerArr.mkString(","))
    //    println("PCA:"+reduceDim.mkString(","))
    val preVector = Vectors.dense(reduceDim)
    val outArr = gmm.predict(preVector, jgmm)

    //删除第二个字段 概率密度
    val tmp = outArr.toBuffer
    tmp.remove(1)
    //反标准变换
    tmp.toArray.map(x => x * std(std.length - 1) + mean(mean.length - 1))
  }

  /**
    * 针对多条数据预测，输出数据要求可以回溯记录顺序
    *
    * @param in 输入数据RDD，要求与训练数据结构一致，包括features和预测变量
    * @return 英文逗号分隔数组，行标号 实际值 预测值 下界 上界
    */
  def checkModel(in: RDD[Array[Double]]): RDD[String] = {
    val timeformat = "00"
    val df = new DecimalFormat(timeformat);
    val out = in.map(x => {
      //前五个字段是时间：Year,Month,Day,Hour,Minute
      val yr = df.format(x(0))
      val mon = df.format(x(1))
      val day = df.format(x(2))
      val hour = df.format(x(3))
      val min = df.format(x(4))
      val rowSeq = yr.concat(mon).concat(day).concat(hour).concat(min)

      rowSeq + "," + x(x.length - 1) + "," + predict(x.dropRight(1)).mkString(",")

    })

    //月、日、时、分、实际值、预测值、下界、上界
    out

  }

  //  private def getCorrColArr(inArr: Array[Double]): Array[Double] = {
  //    val corrScalerArr = new Array[Double](colIndex.length)
  //    for(i <- 0 to corrScalerArr.length - 1 ){
  //      corrScalerArr(i) = inArr(colIndex(i))
  //    }
  //    corrScalerArr
  //  }

  private def getScaler(in: Array[Double]): Array[Double] = {

    new StandardScalerExt().getScaler(in, mean, std)

  }
}

class GMMPredictorWithCorr(tgt: String,
                           hostnode: String) extends Serializable with PredictTrait{

  //价值gmm模型
  val gmmModelPath = MLPathUtil.gmmModelPath(tgt, hostnode)
  val gmm = new MLlibGaussianMixture()
  val jgmm = gmm.getGMM(gmmModelPath)

  //价值标准化模型
  val stdScalerExt = new StandardScalerExt()
  val scalerModelPath = MLPathUtil.scalerModelPath(tgt, hostnode)
  val scalerModel = stdScalerExt.getModel(scalerModelPath)
  val mean = scalerModel.mean.toArray
  val std = scalerModel.std.toArray

  //关系矩阵法筛选的变量
  //  val corrDataCols = PropertyUtil.getModelProperty(MLPathUtil.corrFeatureColsKey(tgt,hostnode)).split(",")
  val corrDataCols = DataFrameUtil.getCsvHeader(MLPathUtil.corrDataPath(tgt, hostnode)).dropRight(1)
  val columns = corrDataCols

  //所有输入变量名及关系矩阵法筛选的变量对应索引，featureCols包含了最后一列的待预测字段名称
  //  val featureCols = PropertyUtil.getModelProperty(MLPathUtil.featureColsKey(tgt,hostnode)).split(",")
  val featureCols = DataFrameUtil.getCsvHeader(MLPathUtil.scalerDataPath(tgt, hostnode))
  val colIndex = new Array[Int](columns.length)
  for (i <- 0 to columns.length - 1) {
    colIndex(i) = featureCols.indexOf(columns(i))
  }

  /**
    * 针对单条数据预测
    *
    * @param data 输入原始features数据，要求与训练数据结构一致
    * @return 英文逗号分隔数组，预测值 下界 上界
    */
  def predict(data: Array[Double]): Array[Double] = {

    val in = data
    val scalerArr = getScaler(in)
    val reduceDim = getCorrColArr(scalerArr)
    val preVector = Vectors.dense(reduceDim)
    val outArr = gmm.predict(preVector, jgmm)

    //删除第二个字段 概率密度
    val tmp = outArr.toBuffer
    tmp.remove(1)
    //反标准变换
    tmp.toArray.map(x => x * std(std.length - 1) + mean(mean.length - 1))
  }

  def sensitivityAnalysis(data: Array[Double],
                          sen_col: Int,
                          sen_rate: Double): Array[Double] = {


    var data_1 = data
    data_1(sen_col) = data_1(sen_col) * (1 - sen_rate)

    var data_2 = data
    data_2(sen_col) = data_2(sen_col) * (1 + sen_rate)

    val rst1 = predict(data_1)
    val rst2 = predict(data_2)

    //未变化数据的预测值、下调sen_rate的预测值、上调sen_rate的预测值
    Array[Double](rst1(0), rst2(0))

  }

  def sensitivityAnalysis(data: Array[Double],
                          sen_rate: Double): Array[Double] = {

    val senArr = new Array[Double](colIndex.length * 2)
    val finishFlag = new Array[Boolean](colIndex.length * 2).map(x => false)

    for (i <- 0 until colIndex.length) {

      val sen_col = colIndex(i)
      val data_1 = data.clone()
      data_1(sen_col) = data(sen_col) * (1 - sen_rate)

      val data_2 = data.clone()
      data_2(sen_col) = data(sen_col) * (1 + sen_rate)

      new Thread(new Runnable {
        override def run(): Unit = {
          val rst1 = predict(data_1)
          senArr(2 * i) = rst1(0) //下调sen_rate的预测值
          finishFlag(2 * i) = true
        }
      }).start()

      new Thread(new Runnable {
        override def run(): Unit = {
          val rst2 = predict(data_2)
          senArr(2 * i + 1) = rst2(0) //上调sen_rate的预测值
          finishFlag(2 * i + 1) = true
        }
      }).start()

    }

    while (finishFlag.contains(false)) {
      println("========>还剩余" + finishFlag.filter(x => !x).length + "个切片分析线程，字段个数：" + colIndex.length)
      Thread.sleep(200)
    }

    senArr
  }

  //与checkModel输入数据一致
  def sensitivityAnalysis(in: RDD[Array[Double]],
                          sen_col: Int,
                          sen_rate: Double): RDD[String] = {
    val timeformat = "00"
    val df = new DecimalFormat(timeformat)
    val out = in.map(x => {
      //前五个字段是时间：Year,Month,Day,Hour,Minute
      val yr = df.format(x(0))
      val mon = df.format(x(1))
      val day = df.format(x(2))
      val hour = df.format(x(3))
      val min = df.format(x(4))
      val rowSeq = yr.concat(mon).concat(day).concat(hour).concat(min)

      rowSeq + "," + x(x.length - 1) + "," + predict(x.dropRight(1))(0) + "," + sensitivityAnalysis(x.dropRight(1), sen_col, sen_rate).mkString(",")

    })

    //月、日、时、分、实际值、未变化数据的预测值、下调sen_rate的预测值、上调sen_rate的预测值
    out

  }

  //与checkModel输入数据一致
  def sensitivityAnalysis(in: RDD[Array[Double]],
                          sen_rate: Double): RDD[String] = {
    val timeformat = "00"
    val df = new DecimalFormat(timeformat)
    val out = in.map(x => {
      //前五个字段是时间：Year,Month,Day,Hour,Minute
      val yr = df.format(x(0))
      val mon = df.format(x(1))
      val day = df.format(x(2))
      val hour = df.format(x(3))
      val min = df.format(x(4))
      val rowSeq = yr.concat(mon).concat(day).concat(hour).concat(min)

      val actual = x(x.length - 1)
      val pre = predict(x.dropRight(1))(0) //原数据预测值

      val senPredict = sensitivityAnalysis(x.dropRight(1), sen_rate) //感度预测值
      val deltaPredict = senPredict.map(x => x - pre) //变化值
      val deltaPredictRate = deltaPredict.map(x => x / pre) //相对变化率
      val deltaPredictRateAbs = deltaPredictRate.map(x => x / sen_rate * 100) //绝对变化率

      //斜率 (应该使用scaler之后的数据处理)
      //      val slop = new Array[Double](x.length)
      //      for(i<-0 until x.length){
      //        slop(i)=(deltaPredict(2*i+1)-deltaPredict(2*i))/x(i)*sen_rate*2
      //      }

      rowSeq + "," + actual + "," + pre + "," + deltaPredictRate.mkString(",")

    })

    //月、日、时、分、实际值、未变化数据的预测值、下调sen_rate的预测值、上调sen_rate的预测值
    out

  }

  /**
    * 针对多条数据预测，输出数据要求可以回溯记录顺序
    *
    * @param in 输入数据RDD，要求与训练数据结构一致，包括features和预测变量
    * @return 英文逗号分隔数组，行标号 实际值 预测值 下界 上界
    */
  def checkModel(in: RDD[Array[Double]]): RDD[String] = {
    val timeformat = "00"
    val df = new DecimalFormat(timeformat)
    val out = in.map(x => {
      //前五个字段是时间：Year,Month,Day,Hour,Minute
      val yr = df.format(x(0))
      val mon = df.format(x(1))
      val day = df.format(x(2))
      val hour = df.format(x(3))
      val min = df.format(x(4))
      val rowSeq = yr.concat(mon).concat(day).concat(hour).concat(min)

      rowSeq + "," + x(x.length - 1) + "," + predict(x.dropRight(1)).mkString(",")

    })

    //月、日、时、分、实际值、预测值、下界、上界
    out

  }

  private def getCorrColArr(inArr: Array[Double]): Array[Double] = {
    val corrScalerArr = new Array[Double](colIndex.length)
    for (i <- 0 to corrScalerArr.length - 1) {
      corrScalerArr(i) = inArr(colIndex(i))
    }
    corrScalerArr
  }

  private def getScaler(in: Array[Double]): Array[Double] = {

    new StandardScalerExt().getScaler(in, mean, std)

  }
}
