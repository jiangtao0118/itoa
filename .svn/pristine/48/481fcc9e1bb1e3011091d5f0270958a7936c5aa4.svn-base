package com.wisdom.spark.ml.features

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil, HDFSFileUtil, MLPathUtil}


/**
  * Created by wisdom on 2016/11/20.
  */
class MLlibPCA extends Serializable{


  val eigenFile = "/eigen/eigenProportion.csv"
  val pcMatFile = "/pc/pcMatrix.csv"
  val corrPCFile = "/pccorr/corrMatrix.csv"

  private def getEigenFile(modelPath:String): String = {
    modelPath + eigenFile
  }
  private def getPcMatFile(modelPath:String): String = {
    modelPath + pcMatFile
  }
  private def getCorrPCFile(modelPath:String): String = {
    modelPath + corrPCFile
  }

  val scalerPCModelFile = "/pcscaler/model"
  val scalerPCDataFile = "/pcscaler/data"
  private def getScalerPCModelFile(modelPath:String): String = {
    modelPath + scalerPCModelFile
  }
  private def getScalerPCDataFile(modelPath:String): String = {
    modelPath + scalerPCDataFile
  }

  def trainAndTransformAndSave(inPath: String,
                               cumulativeThreshold: Double,
                               modelPath: String,
                               outDataPath: String): Unit = {
    val eigenvaluePath = getEigenFile(modelPath)
    val pcPath = getPcMatFile(modelPath)
    val corrPath = getCorrPCFile(modelPath)

    val fdata=DataFrameUtil.readCsv(inPath)
    val feturesVector=DataFrameUtil.dfToRDDofArray(fdata).map(x=>Vectors.dense(x.dropRight(1)))
    val rowMat = new RowMatrix(feturesVector)

    // 既是PCA之前的全部特征数(不包含最后的特征向量)，又是PCA之后特征向量的长度
    // 要求行数（样本数）大于列数（特征数）
    val k = rowMat.numCols().toInt
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rowMat.computeSVD(k, computeU = false)
    //    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: org.apache.spark.mllib.linalg.Vector = svd.s // The singular values are stored in a local dense vector.
    val v: Matrix = svd.V // The V factor is a local dense matrix.

    val pc1 = rowMat.computePrincipalComponents(10)

    //保存特征值
    var eigPr = "component,eigenvalue,proportion,cumulative" +"\n"
    var pccnt=0
    for(i <- 0 until k) {
      val component = i + 1
      val eigenvalue = s(i)
      val proportion = s(i)/s.toArray.sum
      val cumulative = s.toArray.take(i+1).sum/s.toArray.sum

      if(pccnt==0 && cumulative>=cumulativeThreshold){
        pccnt = component
      }

      eigPr = eigPr + component+","+eigenvalue+","+proportion+","+cumulative + "\n"
    }

    HDFSFileUtil.write(eigenvaluePath,eigPr,true)

    //生成PC相关schema，用于存储特征向量、投影后向量
    val pfield = Array.ofDim[StructField](pccnt)
    for(i<-0 until pccnt){
      pfield(i)=StructField("pcVec"+i,DoubleType,false)
    }

    //存储特征向量
    val pcArr = v.toArray.take(pccnt*k)
    val pc = new DenseMatrix(k,pccnt,pcArr)
    writeMatrixToCsv(pc, pfield.map(x=>x.name).mkString(","),pcPath)


    val featureAndPCArrRdd = DataFrameUtil.dfToRDDofArray(fdata).map(y=>{
      //fdata中包含预测变量字段，而计算PC时不包含此字段，所以转换时要减去最后一个字段
      val pcArr = transform(y.dropRight(1),pc)

      //将投影后的（数组表示）点向量添加到最后
      y ++ pcArr
    })

    //PC变量与原始变量关联分析，存储到磁盘的关系矩阵无法保留矩阵中的顺序？？
    val corr=Statistics.corr(featureAndPCArrRdd.map(x=>Vectors.dense(x)))
    writeMatrixToCsv(corr, (fdata.columns++pfield.map(x=>x.name)).mkString(","),corrPath)


    //在PC相关的field基础上增加fdata最后一列的预测变量
    val pStruct = new StructType(pfield).add(fdata.schema.fields(k))

    //取出最后的投影点，追加最后的预测变量
    val projected = featureAndPCArrRdd.map(x=>x.takeRight(pccnt):+x(x.length-pccnt-1))
    val projectedRDD = DataFrameUtil.rddOfArrayToDF(projected,pStruct)
    DataFrameUtil.writeCsv(projectedRDD,outDataPath)

//    DataFrameUtil.writeCsv(projectedRDD,getScalerPCDataFile(modelPath))
//    val scale = new StandardScalerExt()
//    scale.transformAndSave(getScalerPCDataFile(modelPath),
//      outDataPath,
//      getScalerPCModelFile(modelPath))

  }

  def transform(data: Array[Double],pcMatrix: Matrix): Array[Double] = {
    val projected = new Array[Double](pcMatrix.numCols)

    for(i <- 0 until pcMatrix.numCols){
      projected(i) = {
        var sum=0.0
        //pcMatrix.numRows
        for(j <- 0 until pcMatrix.numRows){
          sum+=data(j)*pcMatrix.apply(j,i)
        }
        sum
      }
    }
    projected
  }

  //使用MLlib中的PCA方法计算pc，与通过SVD方法得到的pc互相验证
  //此方法中没有保存模型
  def trainAndSaveWithPCA(inPath: String,
                          k: Int,
                          modelPath: String,
                          outDataPath: String): Unit = {

    val fdata=DataFrameUtil.readCsv(inPath)
    val df = DataFrameUtil.dfToDFofVector(fdata,fdata.columns.dropRight(1), "features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(df)
    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")

    val pfield = Array.ofDim[StructField](k)
    for(i<-0 until k){
      pfield(i)=StructField("pcVec"+i,DoubleType,false)
    }

    DataFrameUtil.writeCsv(DataFrameUtil.dfofVectorToDF(result,new StructType(pfield)),outDataPath)
  }

//  def transformAndScaler(data: Array[Double],
//                         pcMatrix: Matrix,
//                         PCAmodelPath: String): Array[Double] = {
//
//    val p = transform(data,pcMatrix)
//    val scalerModel = StandardScalerModel.load(getScalerPCModelFile(PCAmodelPath))
//    val mean = scalerModel.mean.toArray
//    val std = scalerModel.std.toArray
//
//    new StandardScalerExt().getScaler(p,mean,std)
//  }

  def getPCMatrix(modelPath: String): Matrix = {
    var matArr = new Array[Double](0)
    val pcPath = getPcMatFile(modelPath)
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(pcPath)
    val is = fs.open(dst)
    val br = new BufferedReader(new InputStreamReader(is))
    val header = br.readLine()
    val numCols = header.split(",").length
    var numRows = 0
    var row = br.readLine()
    while(row!=null){

      if(row.trim.length()!=0){
        numRows += 1
        val r = row.split(",").map(x=>x.toDouble)
        matArr = matArr ++ r
        row = br.readLine()
      }
    }

    fs.close()
    new DenseMatrix(numCols,numRows,matArr).transpose
  }


  /**
    * 1 3 5
    * 2 4 6
    * @param m
    * @return
    */
  private def toRDD(m:Matrix): RDD[Array[Double]] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose //skip this if you want a column-major RDD
    val arrs = rows.map(x=>x.toArray)
    ContextUtil.sc.parallelize(arrs)
  }

  private def writeMatrixToCsv(m:Matrix,header:String,path: String): Unit ={
    var all = header + "\n"
    val rows = m.transpose.toArray.grouped(m.numCols)
    while(rows.hasNext){
      all = all + rows.next().mkString(",") + "\n"
    }
    HDFSFileUtil.write(path,all,true)
  }

}

object MLlibPCA{

  val usage ="""
Usage:spark-submit --master yarn-client [spark options] --class MLlibPCA JSparkML.jar <command args>

<command args> :
  trainAndTransformAndSave targetVar hostnode cumulativeThreshold 使用StandardScaler后训练PCA模型
               |targetVar 预测指标
               |hostnode 主机节点
               |cumulativeThreshold 主成份筛选阈值(0-1)区间的小数值，值越大筛选的变量越多
"""
  def main(args: Array[String]): Unit = {

    val sc = ContextUtil.sc

    if (args.length == 0) {
      help()
    }

    val pca = new MLlibPCA()
    val method = args(0)
    if(method.equalsIgnoreCase("trainAndTransformAndSave")){
      if (args.length != 4){
        help()
      }
      val tgt = args(1)
      val hostnode = args(2)
      val cumulativeThreshold = args(3).toDouble

      val inPath = MLPathUtil.scalerDataPath(tgt,hostnode) //标准化后的数据
      val modelPath = MLPathUtil.pcaModelPath(tgt,hostnode)
      val outDataPath = MLPathUtil.pcaDataPath(tgt,hostnode)

      pca.trainAndTransformAndSave(inPath,
        cumulativeThreshold,
        modelPath,
        outDataPath
      )

    }

    ContextUtil.sc.stop()

  }

  def help(): Unit = {
    println(usage)
    System.exit(1)
  }
}
