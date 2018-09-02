package com.wisdom.spark.ml.features

import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.HashMap

/**
  * Created by wisdom on 2016/12/26.
  */
class MutualInfo(crossTable: Matrix) {
  val sumAll = crossTable.toArray.sum

  @transient
  val logger = ContextUtil.logger

  //Row
  def entropyRow(): Double = {

    val entropies = new Array[Double](crossTable.numRows)
    for (i <- 0 until crossTable.numRows) {
      var rowSum = 0.0
      for (j <- 0 until crossTable.numCols) {
        rowSum += crossTable(i, j)
      }
      val freq = 1.0 * rowSum / sumAll
      if (math.abs(freq) < 1e-4) {
        entropies(i) = 0
      }
      else {
        entropies(i) = -1 * freq * math.log(freq)
      }

    }
    entropies.sum
  }

  //Col
  def entropyCol(): Double = {

    val entropies = new Array[Double](crossTable.numCols)
    for (i <- 0 until crossTable.numCols) {
      var colSum = 0.0
      for (j <- 0 until crossTable.numRows) {
        colSum += crossTable(j, i)
      }
      val freq = 1.0 * colSum / sumAll
      if (math.abs(freq) < 1e-4) {
        entropies(i) = 0
      }
      else {
        entropies(i) = -1 * freq * math.log(freq)
      }

    }
    entropies.sum
  }

  //Joint
  def entropyJoint(): Double = {

    var entropy = 0.0
    for (i <- 0 until crossTable.numRows) {
      for (j <- 0 until crossTable.numCols) {
        val freq = 1.0 * crossTable(i, j) / sumAll
        if (math.abs(freq) > 1e-4) {
          entropy += -1 * freq * math.log(freq)
        }

      }
    }
    entropy
  }

  def mutualInfo(): Double = {

    entropyRow() + entropyCol() - entropyJoint()

  }

  def entropyMax(k: Int): Double = {
    -1.0 * math.log(1.0 / k)
  }

}

class MutualMatrix {
  @transient
  val logger = ContextUtil.logger

  //互信息矩阵，DataFrame任意两个字段的互信息
  def mutualMat(data: DataFrame, splits: Int): Matrix = {
    val bucketizer = new MLBucketizer
    val cols = data.columns
    var buked = data

    val tmpPath = "C:\\Users\\wisdom\\Desktop\\tmpBucketed\\"
    for (i <- 0 until cols.length) {
      buked = bucketizer.discretize(buked, cols(i), "buked" + cols(i), splits)
        .drop(cols(i))

      if (i % 10 == 0) {
        DataFrameUtil.writeCsv(buked, tmpPath)
        buked = ContextUtil.sqlContext.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", true.toString)
          .load(tmpPath).cache()

        //        var rdd = buked.rdd
        //        buked = ContextUtil.sqlContext.createDataFrame(rdd,buked.schema).cache()
      }

    }
    val miMat = breeze.linalg.DenseMatrix.ones[Double](cols.length, cols.length)

    var tmp1: Long = 0
    var tmp2: Long = 0

    for (i <- 0 until cols.length) {

      for (j <- 0 until i + 1) {
        tmp1 = System.currentTimeMillis()

        //比DataFrame提供方法效率高，buked.stat.crosstab("buked"+cols(i),"buked"+cols(j))
        val mat = crossTab(buked, "buked" + cols(i), "buked" + cols(j), splits)

        tmp2 = System.currentTimeMillis()
        println(tmp2 - tmp1)

        val mutualInfo = new MutualInfo(mat)
        val mi = mutualInfo.mutualInfo()
        miMat.update(i, j, mi)
        miMat.update(j, i, mi)
        println(i + "," + j + ":" + mi)
        //        miMat.update(i,i,mutualInfo.entropyRow())
      }
    }
    new DenseMatrix(cols.length, cols.length, miMat.toArray)
  }

  //计算DataFrame中每个字段与col的互信息
  def mutualArr(data: DataFrame, col: String, splits: Int): Array[Double] = {
    val bucketizer = new MLBucketizer
    val cols = data.columns
    val tgtColIdx = cols.indexOf(col)
    var buked = data

    logger.info("========>离散化开始：" + System.currentTimeMillis())
    //    val tmpPath="C:\\Users\\wisdom\\Desktop\\tmpBucketed\\"
    //    for(i<-0 until cols.length){
    //      buked = bucketizer.discretize(buked,cols(i),"buked"+cols(i),splits)
    //        .drop(cols(i))
    //
    //      //避免DataFrame迭代中耗时指数增长
    //      //https://issues.apache.org/jira/browse/SPARK-13346
    //      if(i%10==0){
    //        DataFrameUtil.writeCsv(buked,tmpPath)
    //        buked=ContextUtil.sqlContext.read.format("com.databricks.spark.csv")
    //          .option("header","true")
    //          .option("inferSchema",true.toString)
    //          .load(tmpPath).cache()
    //
    //        //        var rdd = buked.rdd
    //        //        buked = ContextUtil.sqlContext.createDataFrame(rdd,buked.schema).cache()
    //      }
    //
    //    }

    //使用RDD并行分桶，比上面的方法效率更高
    buked = bucketizer.quantileDiscretize(data, splits)
    buked.cache()
    logger.info("========>离散化结束：" + System.currentTimeMillis())

    val miArr = new Array[Double](cols.length)

    for (j <- 0 until cols.length) {

      //比DataFrame提供方法效率高，buked.stat.crosstab("buked"+cols(i),"buked"+cols(j))
      //crossTab2使用RDD实现列联表，效率高于使用DataFrame的crossTab
      val mat = crossTab2(buked, "buked" + cols(j), "buked" + col, splits)

      val mutualInfo = new MutualInfo(mat)
      val mi = mutualInfo.mutualInfo()
      miArr(j) = mi
      logger.info("========>MutualInfo：" + j + "," + mi)
      //        miMat.update(i,i,mutualInfo.entropyRow())
    }
    miArr
  }

  def crossTab(data: DataFrame, col1: String, col2: String, splits: Int): Matrix = {
    var tmp1: Long = 0
    var tmp2: Long = 0
    tmp1 = System.currentTimeMillis()

    val cntArr = data.groupBy(col1, col2).count().collect()

    tmp2 = System.currentTimeMillis()
    println(tmp2 - tmp1)
    val freqMat = breeze.linalg.DenseMatrix.zeros[Double](splits, splits)
    for (i <- 0 until cntArr.length) {
      freqMat.update(
        math.round(cntArr(i).getDouble(0)).toInt,
        math.round(cntArr(i).getDouble(1)).toInt,
        cntArr(i).getLong(2).toDouble)
    }

    new DenseMatrix(splits, splits, freqMat.toArray)
  }

  def crossTab2(data: DataFrame, col1: String, col2: String, splits: Int): Matrix = {
//    var tmp1: Long = 0
//    var tmp2: Long = 0
//    tmp1 = System.currentTimeMillis()

    val cntArr = data.select(col1, col2).map(x => {
      val colCntMap = scala.collection.mutable.Map[Int, Int]()
      colCntMap.put(math.round(x.getDouble(1)).toInt, 1)
      (math.round(x.getDouble(0)).toInt, colCntMap)
    }).reduceByKey(
      (a, b) =>
        a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0)) }
    ).collect()

//    tmp2 = System.currentTimeMillis()
//    println(tmp2 - tmp1)
    val freqMat = breeze.linalg.DenseMatrix.zeros[Double](splits, splits)
    for (i <- 0 until cntArr.length) {
      val rowC = cntArr(i)._1
      val cols = cntArr(i)._2.toArray
      for(j<-0 until cols.length){
        freqMat.update(rowC,cols(j)._1,cols(j)._2)
      }
    }

    new DenseMatrix(splits, splits, freqMat.toArray)
  }

}

object MutualMatrix {
  def main(args: Array[String]) {
    val f = "C:\\Users\\wisdom\\Desktop\\IDLE_CPU_FT_2H\\ASCECUP01\\mid\\scaler\\data"
    val df = DataFrameUtil.readCsv(f)

    val ma = new MutualMatrix
    //    val miMat = ma.mutualMat(df,5)
    //    var corrStr = new StringBuilder
    //    for(i<-0 until miMat.numRows){
    //      for(j<-0 until miMat.numCols){
    //        corrStr.append(miMat(i,j) + ",")
    //      }
    //      corrStr.append("\n")
    //    }
    //    ContextUtil.logger.info(corrStr.toString())

    ma.mutualArr(df, df.columns.last, 5)

  }
}
