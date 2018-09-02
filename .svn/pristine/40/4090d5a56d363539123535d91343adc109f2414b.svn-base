package com.wisdom.spark.ml.model.mlpc

import java.text.DecimalFormat
import java.util

import com.wisdom.spark.ml.features.StandardScalerExt
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}
import com.wisdom.spark.ml.mlUtil._

import scala.util.Random

/**
  * Created by wisdom on 2016/11/18.
  */
class MLPR extends Serializable {

  def trainAndSave(inPath: String,
                   modelPath: String,
                   scalerModelPath: String,
                   maxIter: Int,
                   tolerance: Double,
                   interLevels: String
                  ): MultilayerPerceptronClassificationModel = {

    //价值标准化模型
    val stdScalerExt = new StandardScalerExt()
    val scalerModel = stdScalerExt.getModel(scalerModelPath)
    val mean = scalerModel.mean.toArray
    val std = scalerModel.std.toArray

    val intermediate = interLevels.split(",").map(x => x.toInt)

    val maxL = 100 //使用z-score方法标准化数据时取正负3为上下限，数据落入正负3σ区间内的概率为99.7%
    val minL = 0
    val step = 1
    val format = "0"
    val df = new DecimalFormat(format)

    //读取文件
    val fdata = DataFrameUtil.readCsv(inPath)

    //将label列（数据最后一列）转换为format格式，原因是MLC模型中的label列代表类标签而且与输出层的节点个数有关
    //label列的不同的数据个数不能超过输出层节点个数
    val frdd = DataFrameUtil.dfToRDDofArray(fdata)
    val lbrdd = frdd.map(x => x(x.length - 1))
    val maxLabel = lbrdd.max()
    val minLabel = lbrdd.min()

    val cutData = frdd.map(x=>x.dropRight(1) :+ df.format(x(x.length-1)*std(std.length-1) + mean(mean.length-1)).toDouble)
//    val cutData = frdd.map(x => x.dropRight(1) :+ df.format((x(x.length - 1) - minLabel) / (maxLabel - minLabel) * maxL + minL).toDouble)
    val cutDF = DataFrameUtil.rddOfArrayToDF(cutData, fdata.schema)
    val labelFeatureDF = DataFrameUtil.dfToDfOfLabelFeature(cutDF)

    val inputFeatures = fdata.columns.length - 1 //输入特征的个数
    val outputClasses = ((maxL - minL) / step).toInt + 1
    val layers = Array(inputFeatures) ++ intermediate :+ outputClasses

    val trainingData = labelFeatureDF

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setMaxIter(maxIter)
      .setTol(tolerance)
    // train the model
    val model = trainer.fit(trainingData)

//    val result = model.transform(trainingData).select("label", "prediction")
//    val rstArr = DataFrameUtil.dfToRDDofArray(result)
//    val testMSE = rstArr.map(x => math.pow(x(0) - x(1), 2)).mean()
//    rstArr.foreach(x => println(x.mkString(",")))
//    rstArr.map(x => x.mkString(",")).saveAsTextFile("C:\\Users\\wisdom\\Desktop\\testPre")

    saveModel(model, modelPath)
    model

  }

  def predict(x: Array[Double],
              mlpModel: MultilayerPerceptronClassificationModel): Double = {

    val pfield = Array.ofDim[StructField](1)
    pfield(0) = new StructField("features", new VectorUDT(), false)
    val astruct = new StructType(pfield)

    val sc = new SparkContext()
    val row = sc.parallelize(Array(x)).map(x => Vectors.dense(x)).map(x => Row.fromSeq(Seq(x)))
    val df = new SQLContext(sc).createDataFrame(row, astruct)

    df.cache()

    mlpModel.transform(df).select("prediction").first().getAs[Double](0)

  }

  def saveModel(model: MultilayerPerceptronClassificationModel, modelPath: String): Unit = {
    val fileUri = modelPath + "/" + "mlpModel.obj"
    PersitObjectUtil.writeObjectToFile(model, fileUri)
  }

  def getModel(modelPath: String): MultilayerPerceptronClassificationModel = {
    val fileUri = modelPath + "/" + "mlpModel.obj"
    PersitObjectUtil.readObjectFromFile(fileUri).asInstanceOf[MultilayerPerceptronClassificationModel]
  }

}
