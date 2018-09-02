package com.wisdom.spark.ml.features

import com.wisdom.spark.ml.mlUtil.DataFrameUtil
import org.apache.spark.ml.feature.{Bucketizer, IndexToString}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by tup on 2016/12/26.
  */
class MLBucketizer {

  def discretize(in: DataFrame, inCol: String, outCol: String, buckets: Int): DataFrame = {

    val rdd = in.select(inCol).map(x => x.getDouble(0))
    val maxv = rdd.max()
    val minv = rdd.min()
    val interval = (maxv - minv) / (buckets) + 0.01

    val splits = new Array[Double](buckets + 1)
    splits(0) = minv
    for (i <- 1 until splits.length) {
      splits(i) = splits(i - 1) + interval
    }

    val bucketizer = new Bucketizer()
      .setInputCol(inCol)
      .setOutputCol(outCol)
      .setSplits(splits)

    val buked = bucketizer.transform(in)
    buked

    //    val labels = new Array[String](buckets)
    //    for(i<- 0 until buckets){
    //      labels(i)="Class"+i
    //    }
    //    val converter = new IndexToString()
    //      .setInputCol("tmpBucketFeature")
    //      .setOutputCol(outCol)
    //      .setLabels(labels)
    //
    //    converter.transform(buked).drop("tmpBucketFeature")

  }

  def discretize(in: DataFrame, buckets: Int): DataFrame = {

    val cols = in.columns
    val exprCol = new Array[String](cols.length * 2)
    for (i <- 0 until cols.length) {
      exprCol(2 * i) = "max(" + cols(i) + ")"
      exprCol(2 * i + 1) = "min(" + cols(i) + ")"
    }
    val expr = in.selectExpr(exprCol: _*).first()
    val maxium = new Array[Double](cols.length)
    val minimum = new Array[Double](cols.length)
    for (i <- 0 until cols.length) {
      maxium(i) = expr.getDouble(2 * i)
      minimum(i) = expr.getDouble(2 * i + 1)
    }
    val interval = new Array[Double](cols.length)
    for (i <- 0 until cols.length) {
      interval(i) = (maxium(i) - minimum(i)) / (buckets) + 0.01
    }

    val rdd = DataFrameUtil.dfToRDDofArray(in)

    val bukedRDD = rdd.map(x =>
      x.map(y => {
        val idx = x.indexOf(y)
        val max = maxium(idx)
        val min = minimum(idx)
        val intv = interval(idx)
        var out = 0.0
        val splits = new Array[Double](buckets + 1)
        splits(0) = min
        for (i <- 1 until splits.length) {
          splits(i) = splits(i - 1) + intv
        }
        for(i<-0 until buckets){
          if(y>=splits(i) && y<splits(i+1)){
            out = i
          }
        }
        out
      }
      )
    )

    val fieldType = new Array[StructField](cols.length)
    for(i<-0 until cols.length){
      fieldType(i) = new StructField("buked"+cols(i),DoubleType,false)
    }
    val sch = new StructType(fieldType)
    DataFrameUtil.rddOfArrayToDF(bukedRDD, sch)
  }

  //此方法可以使用spark 1.6版本的QuantileDiscretizer方法，1.5版本无此方法，可以使用排序的方法计算分位数。
  //使用分位数的目的之一是消除特别大的值（异常值）的干扰
  //此方法未实现计算分位数，实际上通过抽样的方式可以消除特别大的值（异常值）的干扰
  def quantileDiscretize(in: DataFrame, buckets: Int): DataFrame = {

    val cols = in.columns
    val exprCol = new Array[String](cols.length * 2)
    for (i <- 0 until cols.length) {
      exprCol(2 * i) = "max(" + cols(i) + ")"
      exprCol(2 * i + 1) = "min(" + cols(i) + ")"
    }

    //此方法应使用spark 1.6版本的QuantileDiscretizer方法，1.5版本无此方法，可以使用排序的方法计算。
    //此方法未实现计算分位数，实际上通过抽样的方式可以消除特别大的值（异常值）的干扰
    val fraction = 0.1
    val expr = in.sample(false, fraction,System.currentTimeMillis()).selectExpr(exprCol: _*).first()
//    val expr = in.selectExpr(exprCol: _*).first()
    val maxium = new Array[Double](cols.length)
    val minimum = new Array[Double](cols.length)
    for (i <- 0 until cols.length) {
      maxium(i) = expr.getDouble(2 * i)
      minimum(i) = expr.getDouble(2 * i + 1)
    }
    val interval = new Array[Double](cols.length)
    for (i <- 0 until cols.length) {
      interval(i) = (maxium(i) - minimum(i)) / (buckets) + 0.01
    }

    val rdd = DataFrameUtil.dfToRDDofArray(in)

    val bukedRDD = rdd.map(x =>
      x.map(y => {
        val idx = x.indexOf(y)
        val max = maxium(idx)
        val min = minimum(idx)
        val intv = interval(idx)
        var out = 0.0
        val splits = new Array[Double](buckets + 1)
        splits(0) = min
        for (i <- 1 until splits.length) {
          splits(i) = splits(i - 1) + intv
        }
        for(i<-0 until buckets){
          if(y>=splits(i) && y<splits(i+1)){
            out = i
          }
        }
        out
      }
      )
    )

    val fieldType = new Array[StructField](cols.length)
    for(i<-0 until cols.length){
      fieldType(i) = new StructField("buked"+cols(i),DoubleType,false)
    }
    val sch = new StructType(fieldType)
    DataFrameUtil.rddOfArrayToDF(bukedRDD, sch)
  }

  //  def discretizeQuantile(in: DataFrame, inCol: String, outCol: String, buckets: Int): DataFrame ={
  //
  //    val bucketizer = new QuantileDiscretizer()
  //      .setInputCol(inCol)
  //      .setOutputCol("tmpBucketFeature")
  //      .setSplits(splits)
  //
  //    val buked = bucketizer.transform(in)
  //
  //    val labels = new Array[String](buckets)
  //    for(i<- 0 until buckets){
  //      labels(i)="Class"+i
  //    }
  //    val converter = new IndexToString()
  //      .setInputCol("tmpBucketFeature")
  //      .setOutputCol(outCol)
  //      .setLabels(labels)
  //
  //    val converted = converter.transform(buked)
  //    converted.drop("tmpBucketFeature")
  //
  //  }

}

object MLBucketizer {
  def main(args: Array[String]) {
    val buk = new MLBucketizer
    val df = DataFrameUtil.readCsv("C:\\Users\\wisdom\\Desktop\\entropy.csv")

    val cat = 4
    val buked = buk.discretize(df, "x", "xBucketedFeatures", cat)

    val buked2 = buk.discretize(buked, "normrand", "normrandBucketedFeatures", cat)
    //    val buked2 = buk.discretize(buked,"x_square", "x_squareBucketedFeatures",cat)

    val crossDF = buked2.stat.crosstab("xBucketedFeatures", "normrandBucketedFeatures")
    //    val crossDF = buked2.stat.crosstab("xBucketedFeatures","x_squareBucketedFeatures")

    val mat = DataFrameUtil.dfToMatrix(crossDF.drop(crossDF.columns(0)))

    val mi = new MutualInfo(mat)
    println(mi.entropyRow())
    println(mi.entropyCol())
    println(mi.mutualInfo())
    println(mi.entropyMax(cat))

    println("*********************************")

    val ma = new MutualMatrix
    val miMat = ma.mutualMat(df, 5)
    var corrStr = new StringBuilder
    for (i <- 0 until miMat.numRows) {
      for (j <- 0 until miMat.numCols) {
        corrStr.append(miMat(i, j) + ",")
      }
      corrStr.append("\n")
    }
    println(corrStr.toString())
  }
}