package com.wisdom.spark.ml.mlUtil

import java.util.Calendar

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by wisdom on 2016/11/11.
  * 参考https://github.com/databricks/spark-csv
  */
object DataFrameUtil {

  def main(args: Array[String]): Unit ={

    val data=DataFrameUtil.readCsv("C:\\Users\\wisdom\\Desktop\\test.csv")
    data.printSchema()
  }

  def readCsv(path: String): DataFrame={

    var data=ContextUtil.sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
//      .option("inferSchema",true.toString)
      .load(path)
    val colArray=data.schema.fieldNames

    val pfield = new Array[StructField](colArray.length)
    for(i<-0 until colArray.length){
      pfield(i)=StructField(colArray(i),DoubleType,false)
    }
    val pStruct = new StructType(pfield)

    data = ContextUtil.sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .schema(pStruct)
      .load(path)

    var rtn = data
    if(PropertyUtil.getProperty("readCsv.repartitions")!=null){

      rtn = data.repartition(PropertyUtil.getProperty("readCsv.repartitions").toInt)

    }


    return rtn.cache()
  }

  def getCsvHeader(path:String): Array[String] = {
    ContextUtil.sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load(path).columns
  }

  def writeCsv(data: DataFrame,path: String): Unit={

    data.write.format("com.databricks.spark.csv")
      .option("header","true")
      .option("escape","\\")
      .option("quoteMode","NONE")
      .mode("overwrite")
      .save(path)

//    if(HDFSFileUtil.exists(path)){
//      data.write.format("com.databricks.spark.csv")
//        .option("header","true")
//        .option("escape","\\")
//        .option("quoteMode","NONE")
//        .mode("overwrite")
//        .save(path)
//    }
//    else{
//
//      data.write.format("com.databricks.spark.csv")
//        .option("header","true")
//        .option("escape","\\")
//        .option("quoteMode","NONE")
//        .save(path)
//    }

  }

  def dfToRDDofArray(data: DataFrame): RDD[Array[Double]] ={
    val dfschema=data.schema
    return data.rdd.map(x=>x.toSeq.map(x=>x.toString.toDouble).toArray)
  }

  def rddOfArrayToDF(data: RDD[Array[Double]], schema:StructType): DataFrame = {
    val outRow = data.map(x=>Row.fromSeq(x))
    ContextUtil.sqlContext.createDataFrame(outRow,schema)
  }

  def dfToDfOfLabelFeature(data: DataFrame): DataFrame = {
    val labelCol = data.columns(data.columns.length-1)
    val featuresCols = data.columns.dropRight(1)

    val assembler = new VectorAssembler()
      .setInputCols(featuresCols)
      .setOutputCol("features")

    val vect = assembler.transform(data)
      .withColumnRenamed(labelCol,"label")

    vect.select("label", "features")

  }

  def dfToDfOfFeatures(data: DataFrame): DataFrame = {

    val assembler = new VectorAssembler()
      .setInputCols(data.columns)
      .setOutputCol("features")

    assembler.transform(data).select("features")

  }

  /**
    * VectorAssembler chooses dense vs sparse output format based on whichever one uses less memory.
 *
    * @param data
    * @param inputColsNames
    * @param outColName
    * @return
    */
  def dfToDFofVector(data:DataFrame,inputColsNames: Array[String],outColName: String): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(inputColsNames)
      .setOutputCol(outColName)

    val vect = assembler.transform(data).select(outColName)
    val sch = vect.schema
    val rddOfArray = vect.map{case Row(v:org.apache.spark.mllib.linalg.Vector) => v.toArray}
    val outRow = rddOfArray.map(x=>Row(Vectors.dense(x)))

    ContextUtil.sqlContext.createDataFrame(outRow,sch)
  }

  def dfofVectorToDF(data:DataFrame,schema: StructType): DataFrame = {
    val outArray = data.map{case Row(v:org.apache.spark.mllib.linalg.Vector) => v.toArray.toSeq}
    val outRow = outArray.map(x=>Row.fromSeq(x))
    ContextUtil.sqlContext.createDataFrame(outRow,schema)
  }

  def dfToMatrix(in:DataFrame): Matrix = {
    val arr = dfToRDDofArray(in).collect()

    val rowCnt = arr.length
    val colCnt = arr(0).length

    var matArr=arr(0)
    for(i<-1 until rowCnt){
      matArr = matArr ++ arr(i)
    }
    new DenseMatrix(rowCnt,colCnt,matArr,true)
  }

}
