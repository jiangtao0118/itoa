package com.wisdom.spark.ml.tgtVar

import java.text.DecimalFormat

import com.wisdom.spark.ml.features.StandardScalerExt
import com.wisdom.spark.ml.model.mlpc.MLPR
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.wisdom.spark.ml.mlUtil._

/**
  * Created by wisdom on 2016/11/25.
  */
class MLPPredictorWithCorr(tgt: String,
                   hostnode: String) extends Serializable{

  val mlpr = new MLPR()
  val mlpModel = mlpr.getModel(MLPathUtil.mlpModelPath(tgt,hostnode))

  val stdScalerExt = new StandardScalerExt()
  val scalerModelPath = MLPathUtil.scalerModelPath(tgt,hostnode)
  val scalerModel = stdScalerExt.getModel(scalerModelPath)
  val mean = scalerModel.mean.toArray
  val std = scalerModel.std.toArray

  //关系矩阵法筛选的变量
  val corrDataCols = PropertyUtil.getModelProperty(MLPathUtil.corrFeatureColsKey(tgt,hostnode)).split(",")
  val columns = corrDataCols

  //所有输入变量名及关系矩阵法筛选的变量对应索引，featureCols包含了最后一列的待预测字段名称
  val featureCols = PropertyUtil.getModelProperty(MLPathUtil.featureColsKey(tgt,hostnode)).split(",")
  val colIndex = new Array[Int](columns.length)
  for(i <- 0 to columns.length-1){
    colIndex(i)=featureCols.indexOf(columns(i))
  }

  def checkModel(in: RDD[Array[Double]]): RDD[String] = {
    val sc = in.sparkContext
    val sqlContext = new SQLContext(sc)
    val timeformat="00"
    val df = new DecimalFormat(timeformat)
    in.cache()
    val out = in.map(x=>{
      //前五个字段是时间：Year,Month,Day,Hour,Minute
      val yr = df.format(x(0))
      val mon = df.format(x(1))
      val day = df.format(x(2))
      val hour = df.format(x(3))
      val min = df.format(x(4))
      val rowSeq = yr.concat(mon).concat(day).concat(hour).concat(min)

      rowSeq + "," + x(x.length-1) + "," + predict(x.dropRight(1))

    })

    //月、日、时、分,实际值,预测值
    out
  }

  def predict(data: Array[Double]): String = {

    val in = data

    val scalerArr = getScaler(in)

    val reduceDim = getCorrColArr(scalerArr)

    val rst = mlpr.predict(reduceDim,mlpModel).toString

    rst

  }

  private def getScaler(in: Array[Double]): Array[Double] = {

    new StandardScalerExt().getScaler(in,mean,std)

  }

  private def getCorrColArr(inArr: Array[Double]): Array[Double] = {
    val corrScalerArr = new Array[Double](colIndex.length)
    for(i <- 0 to corrScalerArr.length - 1 ){
      corrScalerArr(i) = inArr(colIndex(i))
    }
    corrScalerArr
  }

}

object MLPPredictor {

    val usage ="""
Usage:spark-submit --master yarn-client [spark options] --class MLPPredictor JSparkML.jar <command args>

<command args> :
  checkModel targetVar hostnode inPath preOutPath method 测试GMM精度
                 |targetVar 预测指标
                 |hostnode 主机节点
                 |inPath 整合后的原始数据
                 |preOutPath 预测输出数据
                 |method CORR 或 PCA
  trainModel targetVar hostnode method maxIter tolerance intermediate   训练MLP模型
                 |targetVar 预测指标
                 |hostnode 主机节点
                 |method CORR 或 PCA
                 |maxIter 最大迭代次数
                 |tolerance 收敛精度
                 |intermediate 中间层节点数，若有多层以英文逗号分隔
               """
    def main(args: Array[String]): Unit = {

      val sc = ContextUtil.sc

      if (args.length == 0) {
        help()
      }

      val method = args(0)
      if(method.equalsIgnoreCase("trainModel")) {
        if (args.length != 7){
          help()
        }
        val tgt = args(1)
        val hostnode = args(2)
        val dimRed = args(3)
        val maxIter = args(4).toInt
        val tolerance = args(5).toDouble
        val intermediate = args(6)

        var inPath = ""
        if(dimRed == "CORR"){
          inPath = MLPathUtil.corrDataPath(tgt,hostnode) //关系矩阵方法降维后的数据
        }
        else if(dimRed == "PCA"){
          inPath = MLPathUtil.pcaDataPath(tgt,hostnode) //PCA方法映射降维后的数据
        }
        else {
          println("******************Not Suport Method***********************:" + dimRed)
        }

        val modelPath: String = MLPathUtil.mlpModelPath(tgt,hostnode)
        val scalerModelPath = MLPathUtil.scalerModelPath(tgt,hostnode)
        val mlp = new MLPR()

        mlp.trainAndSave(inPath,modelPath,scalerModelPath,maxIter,tolerance,intermediate)

      }
      else if(method.equalsIgnoreCase("checkModel")) {

        if (args.length != 6){
          help()
        }

        val tgt = args(1)
        val hostnode = args(2)
        val inPath = args(3)
        val preOutPath = args(4)

        val dimRed = args(5)

        HDFSFileUtil.deleteFile(preOutPath)

        val fdata=DataFrameUtil.readCsv(inPath)
        val fdataRDD=DataFrameUtil.dfToRDDofArray(fdata)

        if(dimRed == "CORR"){
          val mlp = new MLPPredictorWithCorr(tgt,hostnode)
          val out = mlp.checkModel(fdataRDD)
          out.saveAsTextFile(preOutPath)
        }
//        else if(dimRed == "PCA"){
//
//          out.saveAsTextFile(preOutPath)
//        }
        else {
          println("******************Not Suport Method***********************:" + dimRed)
        }

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
}
