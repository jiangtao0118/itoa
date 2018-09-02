package com.wisdom.spark.ml.model.kmeans

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil, HDFSFileUtil}

/**
  * Created by wisdom on 2016/11/30.
  */
class MLKmeans {

  def trainAndSave(inPath: String,
                   modelPath: String,
                   k: Int,
                   maxIter: Int): KMeansModel = {
    val fdata=DataFrameUtil.readCsv(inPath)
    val dataVector=DataFrameUtil.dfToRDDofArray(fdata).map(x=>Vectors.dense(x))

    val kmeans = new KMeans()
      .setK(k)
      .setMaxIterations(maxIter)
      .setSeed(System.currentTimeMillis())

    val model = kmeans.run(dataVector)

    saveModel(model,modelPath)

//    println(model.predict(dataVector).take(10).mkString(","))


    model
  }

  private def saveModel(model:KMeansModel, outPath: String) : Unit = {

    HDFSFileUtil.deleteFile(outPath)
    model.save(ContextUtil.sc,outPath)

  }

}

object MLKmeans{
  def main(args: Array[String]): Unit = {
    val model = new MLKmeans()
      .trainAndSave("C:\\Users\\wisdom\\Desktop\\test.csv",
        "C:\\Users\\wisdom\\Desktop\\kmeans",
        4,
        100
      )

    model.clusterCenters.foreach(println)

//    println(new KMeans().explainParams())
  }
}
