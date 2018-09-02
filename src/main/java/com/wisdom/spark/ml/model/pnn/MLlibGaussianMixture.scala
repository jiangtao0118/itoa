package com.wisdom.spark.ml.model.pnn

import java.util

import org.apache.spark.mllib.clustering.{GaussianMixtureModel, JGM, JGMM, KMeans}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil, HDFSFileUtil, MLPathUtil}
import org.apache.spark.SparkContext

/**
  * Created by wisdom on 2016/11/14.
  */
class MLlibGaussianMixture extends Serializable {

  @transient
  val logger = ContextUtil.logger

  def trainModel(inPath: String,
                 modelPath: String,
                 tolerance: Double,
                 k: Int,
                 maxIter: Int,
                 seed: Long,
                 betaMin: Double,
                 betaStep: Double,
                 betaMax: Double): JGMM = {

    val outPath = modelPath
    val randomSplit = 0.8
    val fdata = DataFrameUtil.readCsv(inPath)
    val dataVector = DataFrameUtil.dfToRDDofArray(fdata).map(x => Vectors.dense(x)).cache()

    val train = dataVector.sample(false, randomSplit, System.currentTimeMillis)
    val test = dataVector.subtract(train)


    val perTimes = 100

    val iters = maxIter / perTimes
    var gm = new JGM().setK(k)

    if (HDFSFileUtil.exists(outPath + "/metadata")) {
      gm = gm.setInitialModel(JGMM.load(ContextUtil.sc, outPath))
    }
    else {
      val (w, center) = getWeightsAndCentersWithKmeans(dataVector, k, maxIter, tolerance, seed)
      logger.info("KMeans初始化权重及中心点============>" + w.mkString(","))
      center.foreach(logger.info)
      gm = gm.setInitialModel(getInitGaussians(w, center))
    }

    //    var gmm = gm
    //      .setConvergenceTol(tolerance)
    //      .setSeed(seed)
    //      .setMaxIterations(perTimes)
    //      .run(train)
    //    saveModel(gmm,outPath)
    //
    //    //增量训练
    //    for(i<-1 until iters){
    //      gmm = new JGM().setK(k)
    //        .setInitialModel(JGMM.load(ContextUtil.sc,outPath))
    //        .setConvergenceTol(tolerance)
    //        .setMaxIterations(perTimes)
    //        .run(train)
    //      saveModel(gmm,outPath)
    //    }

    val gmm = gm
      .setConvergenceTol(tolerance)
      .setSeed(seed)
      .setMaxIterations(maxIter)
      .runDAEM(dataVector, betaMin, betaStep, betaMax)
    //      .run(dataVector)
    saveModel(gmm, outPath)

//    val trainValuesAndPreds = train.map(x => x.toArray).map(x => (x(x.length - 1), predict(Vectors.dense(x.dropRight(1)), gmm)(0)))
//    val trainMetrics = new RegressionMetrics(trainValuesAndPreds)
//    val Ein_MSE = trainMetrics.meanSquaredError
//    val Ein_R2 = trainMetrics.r2
//    val Ein_MAE = trainMetrics.meanAbsoluteError

//    val testValuesAndPreds = test.map(x => x.toArray).map(x => (x(x.length - 1), predict(Vectors.dense(x.dropRight(1)), gmm)(0)))
//    val testMetrics = new RegressionMetrics(testValuesAndPreds)
//    val Eout_MSE = testMetrics.meanSquaredError
//    val Eout_R2 = testMetrics.r2
//    val Eout_MAE = testMetrics.meanAbsoluteError

//    logger.info("Ein_MSE,Eout_MSE: " + Ein_MSE + "," + Eout_MSE)
//    println("Ein_R2=====================>" + Ein_R2)
//    println("Ein_MAE====================>" + Ein_MAE)
//    println("Eout_R2====================>" + Eout_R2)
//    println("Eout_MAE===================>" + Eout_MAE)

    gmm

  }

  /**
    * 使用权重数组、中心点数组、单位矩阵初始化多维高斯分布和GMM模型
    * @param weights
    * @param mu
    * @return
    */
  def getInitGaussians(weights: Array[Double],
                       mu: Array[org.apache.spark.mllib.linalg.Vector]): JGMM = {
    val gArr = new Array[MultivariateGaussian](weights.length)

    for (i <- 0 until weights.length) {
      gArr(i) = new MultivariateGaussian(mu(i), Matrices.eye(mu(i).size))
    }

    new JGMM(weights, gArr)
  }

  /**
    * 使用kmeans获取权重及中心点，用于初始化Gaussian
    * @param dataVector
    * @param k
    * @param maxIter
    * @param tolerance
    * @param seed
    * @return
    */
  def getWeightsAndCentersWithKmeans(dataVector: RDD[org.apache.spark.mllib.linalg.Vector],
                                     k: Int,
                                     maxIter: Int,
                                     tolerance: Double,
                                     seed: Long): (Array[Double], Array[org.apache.spark.mllib.linalg.Vector]) = {

    val kmeans = new KMeans()
      .setK(k)
      .setMaxIterations(maxIter)
      .setRuns(3)
      .setSeed(seed)

    val model = kmeans.run(dataVector)

    val centers = model.clusterCenters

    val preArr = model.predict(dataVector).collect()
    val weights = new Array[Double](centers.length)
    for (i <- 0 until preArr.length) {
      weights(preArr(i)) += 1
    }

    (weights.map(x => x * 1.0 / weights.sum), centers)

  }

  //  def predict(data: RDD[org.apache.spark.mllib.linalg.Vector],modelPath:String): RDD[Array[Double]] = {
  //    val gmm=JGMM.load(ContextUtil.sc,modelPath)
  //    val pre=data.map(x=>getPredict(x,gmm))
  //    pre
  //  }

  //  def predict(data: org.apache.spark.mllib.linalg.Vector,modelPath:String): Array[Double] = {
  //    val gmm=JGMM.load(ContextUtil.sc,modelPath)
  //    val pre=getPredict(data,gmm)
  //    pre
  //  }

  //x中只包含features，不包含label或待预测变量
  def predict(data: org.apache.spark.mllib.linalg.Vector, gmm: JGMM): Array[Double] = {
    val pre = getPredict(data, gmm)
    pre
  }

  //  def predictAndSave(inPath: String,modelPath: String,outPath: String): RDD[Array[Double]] = {
  //    val fdata=DataFrameUtil.readCsv(inPath)
  //    val data=DataFrameUtil.dfToRDDofArray(fdata).map(x=>Vectors.dense(x))
  //    val pre=predictAndSave(data,modelPath,outPath)
  //
  //    pre
  //  }

  def getGMM(modelPath: String): JGMM = {
    val time1 = System.currentTimeMillis()
    val res = JGMM.load(ContextUtil.sc, modelPath)
    val time2 = System.currentTimeMillis()
    logger.info("------------------ JGMM.load(ContextUtil.sc, " + modelPath + ") cost: " + (time2 - time1) + "ms")
    res
  }

  //  private def predictAndSave(data: RDD[org.apache.spark.mllib.linalg.Vector],modelPath: String,outPath: String): RDD[Array[Double]] = {
  //    val pre=predict(data,modelPath)
  ////    case class PredictOutPut(actual: Double,predict: Double,dense: Double,lower: Double,upper: Double)
  ////    val df = ContextUtil.sqlContext.createDataFrame(pre.map({case Array(x0,x1,x2,x3,x4)=>PredictOutPut(x0,x1,x2,x3,x4)}))
  //    val afield = Array.ofDim[StructField](5)
  //    afield(0)=StructField("actual",DoubleType,false)
  //    afield(1)=StructField("predict",DoubleType,false)
  //    afield(2)=StructField("dense",DoubleType,false)
  //    afield(3)=StructField("lower",DoubleType,false)
  //    afield(4)=StructField("upper",DoubleType,false)
  //    val aStruct = new StructType(afield)
  //    val outDF = ContextUtil.sqlContext.createDataFrame(pre.map(x=>x.toSeq).map(x=>Row.fromSeq(x)),aStruct)
  //    DataFrameUtil.writeCsv(outDF,outPath)
  //
  //    pre
  //  }

  //x中只包含features，不包含label或待预测变量
  private def getPredict(x: org.apache.spark.mllib.linalg.Vector, gmm: JGMM): Array[Double] = {

    //    val maxL=0.8 //使用MaxMin方法标准化数据时取上下限
    //    val minL=0.2
    val maxL = 3.0 //使用z-score方法标准化数据时取正负3为上下限，数据落入正负3σ区间内的概率为99.7%
    val minL = -3.0
    val step = 0.005
    val slices = ((maxL - minL) / step).toInt //迭代0.0005=>12000次,0.005=>1200次
    val pdf = Array.ofDim[Double](slices + 1, 2)
    val a = x.toArray.map(x => x.toString.toDouble) :+ 0.0 //追加步长点
    val output = Array.ofDim[Double](5) //actual predict dense lower upper 第一个字段actual为0
    val eachMax = Array.ofDim[Double](2)
    var pdfSum = 0.0

    for (i <- 0 until (slices + 1)) {
      val tmp = minL + i.toDouble * step;
      a(a.size - 1) = tmp
      val result = gmm.predictSoft(Vectors.dense(a))
      var resultSum = result.sum
      pdfSum = pdfSum + resultSum
      if (i == 0) {
        eachMax(0) = tmp
        eachMax(1) = resultSum
      }

      if (resultSum > eachMax(1)) {
        eachMax(0) = tmp
        eachMax(1) = resultSum
      }

      pdf(i)(0) = tmp
      pdf(i)(1) = resultSum
    }

    output(1) = eachMax(0)
    output(2) = eachMax(1)

    val sigma = 0.022 //P（μ-σ<X≤μ+σ）=68.3%P（μ-2σ<X≤μ+2σ）=95.5%P（μ-3σ<X≤μ+3σ）=99.7%
    val pdfUpper = pdfSum * (1 - sigma)
    val pdfLower = pdfSum * sigma
    var pdfSumT = 0.0
    val toosmall: Double = 1E-6

    var upperFlag = 0
    var lowerFlag = 0
    for (i <- 0 until (slices + 1)) {
      pdfSumT = pdfSumT + pdf(i)(1)
      if ((pdfSumT > pdfLower) && (lowerFlag == 0)) {
        output(3) = pdf(i)(0)
        lowerFlag = 1
      }
      if ((pdfSumT > pdfUpper) && (upperFlag == 0)) {
        output(4) = pdf(i)(0)
        upperFlag = 1

      }

      if (output(2) < toosmall && Math.abs(output(0)) < 2) {
        output(1) = output(0) + output(0) * 0.01
        output(3) = output(0) - 1
        output(4) = output(0) + 1
      }
    }

    //返回predict dense lower upper
    return output.drop(1)
  }

  private def saveModel(gmm: GaussianMixtureModel, outPath: String): Unit = {
    //先将模型保存到临时目录，如果保存成功则移动到指定目录，以免误删。
    //    val tmpOutPath = System.currentTimeMillis().toString + "/_TMP"
    //    HDFSFileUtil.deleteFile(tmpOutPath)
    //    gmm.save(ContextUtil.sc,tmpOutPath)
    //    HDFSFileUtil.moveFile(tmpOutPath,outPath)

    HDFSFileUtil.deleteFile(outPath)
    gmm.save(ContextUtil.sc, outPath)

  }

  private def saveModel(gmm: JGMM, outPath: String): Unit = {
    //先将模型保存到临时目录，如果保存成功则移动到指定目录，以免误删。
    //    val tmpOutPath = System.currentTimeMillis().toString + "/_TMP"
    //    HDFSFileUtil.deleteFile(tmpOutPath)
    //    gmm.save(ContextUtil.sc,tmpOutPath)
    //    HDFSFileUtil.moveFile(tmpOutPath,outPath)

    HDFSFileUtil.deleteFile(outPath)
    gmm.save(ContextUtil.sc, outPath)

  }

}

object MLlibGaussianMixture {

  def main(args: Array[String]): Unit = {
    //
    //    val gm = new MLlibGaussianMixture()
    //    val fdata=DataFrameUtil.readCsv("C:\\Users\\wisdom\\Desktop\\test.csv")
    //    val (w,center) = gm.getWeightsAndCentersWithKmeans(fdata,8,1000,0.00005)
    //    println(w.mkString(","))
    //    center.foreach(println)

  }

}