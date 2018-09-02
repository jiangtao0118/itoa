package com.wisdom.spark.ml.model.randomForest

import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil, HDFSFileUtil, MLPathUtil}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row

object  MLlibRandomForestRegression {

  @transient
  val logger = ContextUtil.logger

  def main(args: Array[String]) {
    val inPath = "C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_BIAS_VAR_L.csv"
    val inPath_test = "C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_BIAS_VAR_P.csv"
    val fdata = DataFrameUtil.readCsv(inPath)
    val lfDF = DataFrameUtil.dfToDfOfLabelFeature(fdata)

    val fdata_test = DataFrameUtil.readCsv(inPath_test)
    val lfDF_test = DataFrameUtil.dfToDfOfLabelFeature(fdata_test)

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(8)
      .fit(lfDF)

    val rf = new RandomForestRegressor()
      //featuresCol: features column name (default: features)
      .setFeaturesCol("features")
      //labelCol: label column name (default: label)
      .setLabelCol("label")
      //predictionCol: prediction column name (default: prediction)
      .setPredictionCol("prediction")

    val pipeline = new Pipeline()
      .setStages(Array(rf))
    //      .setStages(Array(featureIndexer, gbt))

    val paramGrid = new ParamGridBuilder()
        .addGrid(rf.featureSubsetStrategy, Array("all")) //"sqrt","onethird"
      .addGrid(rf.maxBins, Array(32))
      .addGrid(rf.maxDepth, Array(5))
      .addGrid(rf.numTrees, Array(120))
      .addGrid(rf.subsamplingRate, Array(1.0)) //0.6, 1.0
      .build()

    val eva = new RegressionEvaluator()
      .setLabelCol(rf.getLabelCol)
      .setPredictionCol(rf.getPredictionCol)
      .setMetricName("mse")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(eva)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(lfDF)

    val params = cv.getEstimatorParamMaps
    val metics = cvModel.avgMetrics
    logger.info("featureSubsetStrategy,maxBins,maxDepth,numTrees,subsamplingRate,metics")
    for(i<-0 until params.length){
      var params_str=""
      params_str = params_str + params(i).get(rf.featureSubsetStrategy).get + ","
      params_str = params_str + params(i).get(rf.maxBins).get + ","
      params_str = params_str + params(i).get(rf.maxDepth).get + ","
      params_str = params_str + params(i).get(rf.numTrees).get + ","
      params_str = params_str + params(i).get(rf.subsamplingRate).get + ","

      logger.info(params_str + metics(i))
    }

    val rfm = cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[RandomForestRegressionModel]
    val featureImportances=rfm.featureImportances.toArray
    logger.info(featureImportances.mkString(","))

    cvModel.transform(lfDF)
      .select("label", "prediction")
      .collect()
      .foreach { case Row(label: Double, prediction: Double) =>
        logger.info(s"$label, $prediction")
      }
  }

}