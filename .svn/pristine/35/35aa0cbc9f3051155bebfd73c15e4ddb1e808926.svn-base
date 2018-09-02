package com.wisdom.spark.ml.features

import com.wisdom.spark.ml.mlUtil.{ContextUtil, DataFrameUtil, MLPathUtil, PropertyUtil}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



/**
  * Created by wisdom on 2016/11/13.
  * 使用相关系数筛选变量
  */
class CorrCovExt {

  @transient
  val logger = ContextUtil.logger

  /**
    *
    * @param method pearson spearman
    * @param threshold 0-1
    * @param inPath 输入文件路径
    * @param outPath 输出文件路径
    * @param whitelistRegx 字段白名单
    * @return (相关矩阵, 过滤后字段名)
    *
    * TODO: RandomForest/GBDT筛选变量 每个变量影响权重
    *
    */
  def corrAndSave(method: String,threshold: Double, inPath: String, outPath: String, whitelistRegx: String): (Matrix,Array[String]) = {

    val fdata=DataFrameUtil.readCsv(inPath)
    val data=DataFrameUtil.dfToRDDofArray(fdata).map(x=>Vectors.dense(x))

    val coThreshold = 0.95

    //计算相关系数矩阵
    val corr=Statistics.corr(data,method)

//    val corrStr = new StringBuilder
//    for(i<-(corr.numRows-1) until corr.numRows){
//      for(j<-0 until corr.numCols){
//        corrStr.append(corr(i,j) + ",")
//      }
//      corrStr.append("\n")
//    }
//    logger.info("相关系数矩阵Correlation Matrix(与目标变量相关系数)=================>")
//    logger.info(corrStr.toString())

    //    取出与预测变量（最后一列）相关的相关系数
    val covarpre= new mutable.HashMap[Int,Double]()

    //使用相关系数筛选
    //取corr的最后一列，去除最后一行的预测变量的相关系数
    for (i <- 0 to (corr.numRows-2) ){
      covarpre.put(i,corr(i,corr.numRows-1))
    }

    //使用互信息筛选
//    val mi = new MutualMatrix().mutualArr(fdata,fdata.columns.last,5)
//    for(i<-0 until corr.numRows-1){
//      covarpre.put(i,mi(i))
//    }

    //去除NaN
    val corrArr = covarpre.values.toArray.map(x=>math.abs(x)).filter(x=>x>=0)
    val desc = new DescriptiveStatistics(corrArr)

    logger.info("目标变量相关系数，不包括自相关：")
    logger.info("相关系数数量(包含NaN)：" + covarpre.size)
    logger.info("相关系数数量(去除NaN)：" + desc.getN)
//    logger.info("相关系数均值：" + desc.getMean)
//    logger.info("相关系数标准差：" + desc.getStandardDeviation)
    logger.info("相关系数中位数：" + desc.getPercentile(0.5))
    logger.info("相关系数75%分位：" + desc.getPercentile(0.75))
    logger.info("相关系数25%分位：" + desc.getPercentile(0.25))
    logger.info("相关系数最大值：" + desc.getMax)
//    logger.info("相关系数最小值：" + desc.getMin)
    logger.info("相关系数最大的前10个：" + corrArr.sortBy(x => -x).take(10).mkString(","))

    //筛选出协方差相关系数大于threshold的列，并给出列号
    var filterDimAndCovar=covarpre.filter(p => Math.abs(p._2)>=threshold)

    val whitelist=new ArrayBuffer[Int]()
    if(whitelistRegx.trim.length>0){
      for(i<-0 until (fdata.columns.length-1)){
        if(whitelistRegx.toLowerCase.r.findFirstIn(fdata.columns(i).toLowerCase).nonEmpty){
          whitelist.append(i)
        }
      }
    }

    val keys = (filterDimAndCovar.keySet.toArray.clone() ++: whitelist.toArray).distinct.sorted
    for(i<-0 until keys.length){
      val j = keys(i)
      for(k<-(j+1) until corr.numRows-1){
        if(math.abs(corr(k,j))>coThreshold && filterDimAndCovar.contains(k)){
          filterDimAndCovar.remove(k)
        }
      }
    }

    var filterDim=(filterDimAndCovar.keySet.toArray.clone() ++: whitelist.toArray).distinct
    if(filterDim.length==0){
      logger.info("没有大于阈值的自变量，取相关系数/互信息最大的5个字段")
      filterDim = covarpre.toSeq.sortBy(-_._2).take(5).map(_._1).toArray
    }

    //可能需要将最后一列的序号追加到数组，此列代表预测变量
    var filterDimL=filterDim:+(corr.numRows-1)
    filterDimL=filterDimL.sorted

    //筛选出filterDimL中序号代表的字段
    val nCols = fdata.columns.filter(x=>filterDimL.contains(fdata.columns.indexOf(x)))
    val nData = fdata.selectExpr(nCols:_*)

    //转换后的数据包含自变量和预测变量
    DataFrameUtil.writeCsv(nData,outPath)

    logger.info("相关系数筛选剩余字段：" + nCols.mkString(","))
    (corr,nCols)
  }

}

object CorrCovExt {

  val usage ="""
Usage:spark-submit --master yarn-client [spark options] --class CorrCovExt JSparkML.jar <command args>

<command args> :
  corrAndSave method threshold targetVar hostnode 相关性分析
               |method:pearson,spearman
               |threshold 相关系数阈值，大于此阈值的相关列会筛选出来
               |targetVar 预测指标
               |hostnode 主机节点
"""

  def main(args: Array[String]): Unit = {

    val sc = ContextUtil.sc

    if (args.length == 0) {
      help()
    }

    val corrExt = new CorrCovExt()
    val fun = args(0)

    if(fun.equalsIgnoreCase("corrAndSave")) {
      if (args.length != 5){
        help()
      }
      val method = args(1)
      val threshold = args(2).toString.toDouble
      val tgt = args(3)
      val hostnode = args(4)
      val inPath = MLPathUtil.scalerDataPath(tgt,hostnode)
      val outDataPath = MLPathUtil.corrDataPath(tgt,hostnode)

      val tgt_ind = tgt.substring(0, tgt.lastIndexOf("_"))
      val whitelistRegx=PropertyUtil.getProperty("ml.whitelist." + tgt_ind)
      val out = corrExt.corrAndSave(method,threshold,inPath,outDataPath,whitelistRegx)
      val corr = out._1

      //输出字段名称时不包含最后一列的预测变量
      val corrCols = out._2.dropRight(1).mkString(",")
      PropertyUtil.saveModelProperty(MLPathUtil.corrFeatureColsKey(tgt,hostnode),corrCols)

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
