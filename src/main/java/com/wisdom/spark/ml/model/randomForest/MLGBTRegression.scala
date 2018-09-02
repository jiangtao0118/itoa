package com.wisdom.spark.ml.model.randomForest

import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row

/**
  * Created by tup on 2017/4/6.
  */
object MLGBTRegression {

  @transient
  val logger = ContextUtil.logger

  def main(args: Array[String]) {
    val inPath = "C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_BIAS_VAR_L.csv"
    val inPath_test = "C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_BIAS_VAR_L.csv"
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

    val gbt = new GBTRegressor()
      //featuresCol: features column name (default: features)
      .setFeaturesCol("features")
      //labelCol: label column name (default: label)
      .setLabelCol("label")
      //predictionCol: prediction column name (default: prediction)
      .setPredictionCol("prediction")
      //lossType: Loss function which GBT tries to minimize (case-insensitive). Supported options: squared, absolute (default: squared)
      .setLossType("squared")
    //maxBins: Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature. (default: 32)
    //      .setMaxBins(32)
    //maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5)
    //      .setMaxDepth(3)
    //maxIter: maximum number of iterations (>= 0) (default: 20)
    //      .setMaxIter(10)
    //maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. (default: 256)
    //      .setMaxMemoryInMB()
    //minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)
    //      .setMinInfoGain(0.1)
    //minInstancesPerNode: Minimum number of instances each child must have after split.  If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1)
    //      .setMinInstancesPerNode(2)
    //stepSize: Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator (default: 0.1)
    //      .setStepSize(0.1)
    //subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)
    //      .setSubsamplingRate(0.6)
    //seed: random seed (default: -131597770)
    //      .setSeed(54321L)
    //impurity: Criterion used for information gain calculation (case-insensitive). Supported options: variance (default: variance)
    //cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. (default: false)
    //checkpointInterval: Specifies how often to checkpoint the cached node IDs.  E.g. 10 means that the cache will get checkpointed every 10 iterations. This is only used if cacheNodeIds is true and if the checkpoint directory is set in the SparkContext. Must be >= 1. (default: 10)

    val pipeline = new Pipeline()
      .setStages(Array(gbt))
    //      .setStages(Array(featureIndexer, gbt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(32))
      .addGrid(gbt.maxDepth, Array(3))
//      .addGrid(gbt.maxDepth, Array(4))
      .addGrid(gbt.maxIter, Array(80))
//      .addGrid(gbt.maxIter, Array(40))
      .addGrid(gbt.stepSize, Array(0.1))
//      .addGrid(gbt.stepSize, Array(0.1))
      .addGrid(gbt.subsamplingRate, Array(1.0))
//      .addGrid(gbt.subsamplingRate, Array(1.0))
      .build()

    val eva = new RegressionEvaluator()
      .setLabelCol(gbt.getLabelCol)
      .setPredictionCol(gbt.getPredictionCol)
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
    logger.info("maxBins,maxDepth,maxIter,stepSize,subsamplingRate,metics")
    for(i<-0 until params.length){
      var params_str=""
      params_str = params_str + params(i).get(gbt.maxBins).get + ","
      params_str = params_str + params(i).get(gbt.maxDepth).get + ","
      params_str = params_str + params(i).get(gbt.maxIter).get + ","
      params_str = params_str + params(i).get(gbt.stepSize).get + ","
      params_str = params_str + params(i).get(gbt.subsamplingRate).get + ","

      logger.info(params_str + metics(i))
    }

    cvModel.transform(lfDF)
      .select("label", "prediction")
      .collect()
      .foreach { case Row(label: Double, prediction: Double) =>
        logger.info(s"$label, $prediction")
      }
  }
}
