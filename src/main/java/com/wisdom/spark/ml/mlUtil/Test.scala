package com.wisdom.spark.ml.mlUtil

import java.io.{FileInputStream, FileOutputStream, PrintStream}
import java.text.DecimalFormat
import java.util
import java.util.{Calendar, Properties}

import com.wisdom.spark.ml.features.{Smoothing1D, StandardScalerExt}
import com.wisdom.spark.ml.model.ft.{Complex, DFT}
import com.wisdom.spark.ml.model.pnn.MLlibGaussianMixture
import com.wisdom.spark.ml.tgtVar.GMMPredictor
import org.apache.commons.math3.distribution.{MultivariateNormalDistribution, NormalDistribution}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{IDF, _}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.sql.Row

/**
  * Created by wisdom on 2016/11/16.
  */
object Test {

  case class log_sentence(text: String, time: String)

  def main(args: Array[String]): Unit = {
    //初始化sc--必需
//    val sc = ContextUtil.sc

    val record="2015-07-16-16.41.08.555250+480 I1537A404            LEVEL: Event\nPID     : 14614742             TID : 1              PROC : db2diag\nINSTANCE: db2iaops             NODE : 000\nHOSTNAME: DBCAOPS01\nEDUID   : 1\nFUNCTION: DB2 UDB, RAS/PD component, pdDiagArchiveDiagnosticLog, probe:88\nCREATE  : DB2DIAG.LOG ARCHIVE : /home/db2iaops/sqllib/db2dump/db2diag.log_2015-07-16-16.41.08 : success\nIMPACT  : Potential"
    val tmp = record.split("\n")
    println(tmp)
//    val lda = new LDA()

//    val tmp="abcdef"
//    println(tmp.substring(0,tmp.length-1))

//    println(Double.NaN.isNaN)
//    val tgtVar="IDLE_CPU_AUTO"
//    val hostnode="ASCECUP01"
//    val trainFile="C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_FT_L.csv"
//
//    val scaler = new StandardScalerExt()
//    val scalerFile = MLPathUtil.scalerDataPath(tgtVar, hostnode)
//    val scalermodelPath = MLPathUtil.scalerModelPath(tgtVar, hostnode)
//
//    scaler.transformAndSave(trainFile,
//      scalerFile,
//      scalermodelPath)
//
//    val fdata=DataFrameUtil.readCsv(scalerFile)
//    val data=DataFrameUtil.dfToRDDofArray(fdata).map(x=>Vectors.dense(x))
//    //计算相关系数矩阵
//    val corr=Statistics.corr(data,"pearson")
//
//    val fdata1=DataFrameUtil.readCsv(trainFile)
//    val data1=DataFrameUtil.dfToRDDofArray(fdata1).map(x=>Vectors.dense(x))
//    //计算相关系数矩阵
//    val corr2=Statistics.corr(data1,"pearson")
//
//    println("after scaler,before scaler")
//    for(i<-0 until corr.numCols-1){
//      println(corr(i,corr.numCols-1) + "," + corr2(i,corr.numCols-1))
//    }

//    val xml="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<soap:Envelope xmlns:s=\"http://esb.spdbbiz.com/services/S010010016\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:d=\"http://esb.spdbbiz.com/metadata\">\n\t<soap:Header>\n\t\t<s:ReqHeader>\n\t\t\t<d:Mac>0000000000000000</d:Mac>\n\t\t\t<d:MacOrgId>9957</d:MacOrgId>\n\t\t\t<d:MsgId>76ace5d2-03df-4a64-9c50-4fe86056ba88</d:MsgId>\n\t\t\t<d:SourceSysId>0014</d:SourceSysId>\n\t\t\t<d:ConsumerId>0014</d:ConsumerId>\n\t\t\t<d:ServiceAdr>http://esb.spdbbiz.com/services/S010010016</d:ServiceAdr>\n\t\t\t<d:ServiceAction>urn:/EbnkAndTelBnkCrnDepTfr</d:ServiceAction>\n\t\t</s:ReqHeader>\n\t</soap:Header>\n\t<soap:Body>\n\t\t<s:ReqEbnkAndTelBnkCrnDepTfr>\n\t\t\t<s:ReqSvcHeader>\n\t\t\t\t<s:TranDate>20170228</s:TranDate>\n\t\t\t\t<s:TranTime>162531069</s:TranTime>\n\t\t\t\t<s:TranTellerNo>99957368</s:TranTellerNo>\n\t\t\t\t<s:TranSeqNo>99572432074</s:TranSeqNo>\n\t\t\t\t<s:ConsumerId>0014</s:ConsumerId>\n\t\t\t\t<s:GlobalSeqNo>00141702271625317854025546</s:GlobalSeqNo>\n\t\t\t\t<s:BranchId>9957</s:BranchId>\n\t\t\t\t<s:TerminalCode>1a</s:TerminalCode>\n\t\t\t\t<s:PIN>7920600273333</s:PIN>\n\t\t\t</s:ReqSvcHeader>\n\t\t\t<s:SvcBody>\n\t\t\t\t<s:AcctType>1</s:AcctType>\n\t\t\t\t<s:ClientAcctNo>6217920602864444</s:ClientAcctNo>\n\t\t\t\t<s:PwdCheckType>2</s:PwdCheckType>\n\t\t\t\t<s:TranPwd>431E8605F1E3B76A</s:TranPwd>\n\t\t\t\t<s:CurrencyId>01</s:CurrencyId>\n\t\t\t\t<s:CashRateFlag>0</s:CashRateFlag>\n\t\t\t\t<s:AcctChar>0001</s:AcctChar>\n\t\t\t\t<s:TranAmt>50000</s:TranAmt>\n\t\t\t\t<s:InAcctType>1</s:InAcctType>\n\t\t\t\t<s:InAcctNo>6217960601129118</s:InAcctNo>\n\t\t\t</s:SvcBody>\n\t\t</s:ReqEbnkAndTelBnkCrnDepTfr>\n\t</soap:Body>\n</soap:Envelope>"
//    val tmp = "<s:AcctType>1</s:AcctType>"
//    println(extractCol(xml))



//    val col="IDLE_CPU_m21_t"
//    val regx=".*cpu.*_(?!m)[0-2].*$"
//    println(regx.toLowerCase.r.findFirstIn(col.toLowerCase).nonEmpty)

//    val dis = new NormalDistribution(0,1.5)
//    println(dis.density(0))
//    println(dis.density(1))
//    println(dis.density(2))

    //傅里叶变换
//    val times="96,95,96,96,96,96,96,96,96,96,96,97," +
//      "97,97,97,97,97,97,97,97,97,97,97,97,98,97,98," +
//      "97,97,97,98,97,98,98,98,98,98,93,97,97,97,96," +
//      "97,98,98,98,98,98,98,98,98,98,98,98,98,98,98," +
//      "98,98,98,98,98,98,98,97,98,98,97,97,97,97,97," +
//      "97,97,96,97,96,96,96,96,96,96,95,95,95,95,95," +
//      "95,95,94,95,93,94,94,92,93,93,93,90,90,88,87," +
//      "86,83,83,84,82,83,80,78,86,79,82,80,78,77,80," +
//      "81,76,75,75,81,78,77,77,76,79,76,74,77,75,79," +
//      "71,73,73,72,73,80,83,78,76,81,82,80,81,82,88," +
//      "81,81,84,82,84,80,80,85,82,82,82,87,83,79,80," +
//      "80,74,81,75,82,77,78,79,74,83,77,74,78,71,69," +
//      "73,77,74,78,71,77,73,80,79,77,79,77,77,76,83," +
//      "78,78,81,82,83,79,77,79,82,84,81,82,82,87,84," +
//      "86,84,82,87,86,83,88,86,87,83,86,88,88,88,89," +
//      "87,89,88,86,88,90,90,89,89,88,89,89,89,90,88," +
//      "88,86,88,89,90,90,89,86,91,89,86,89,90,89,89," +
//      "87,88,87,88,90,88,90,88,90,89,90,90,91,87,91," +
//      "91,92,90,91,91,91,91,93,93,93,92,93,94,94,92," +
//      "94,95,94,95,95,95"
//    val times = "93,95,95,94,95,95,95,95,95,96,96,96,96,96,96,96,96,97,96,96,97,97,96,97,96,97,97,97,97,98,97,98,97,97,97,97,97,98,97,98,97,98,97,98,97,96,96,96,96,98,97,98,97,98,98,98,97,98,97,98,98,98,97,98,97,97,97,97,97,97,97,97,97,97,96,97,96,97,96,97,96,96,96,95,95,94,94,95,95,95,94,94,93,94,93,93,93,91,91,90,90,91,88,87,81,83,79,81,80,78,76,71,76,76,75,78,76,72,76,78,81,71,69,70,67,76,69,73,68,70,69,72,69,70,78,74,73,71,75,82,74,76,77,78,83,77,80,76,81,81,84,80,77,76,82,85,83,78,78,77,77,76,75,75,77,77,78,77,82,74,73,79,74,79,79,79,74,68,66,75,70,68,70,71,77,77,71,76,72,76,79,80,82,70,78,76,77,78,75,77,77,79,78,78,78,75,78,77,78,84,78,82,81,85,83,84,81,81,85,84,87,84,85,86,84,86,85,84,87,88,86,86,89,85,87,88,86,87,86,88,86,85,87,87,85,86,85,87,86,89,86,86,87,86,85,86,89,90,84,89,89,89,88,87,88,86,90,87,89,89,89,88,87,92,90,91,87,91,92,91,90,91,92,94,93,93,94,92,94,94,94,95,94,96,94,93,95,91,95,96,96,96,96,97,96,96,97,97,96,97,96,97,97,97,97,97,97,97,97,98,97,98,97,97,97,97,97,98,93,93,94,94,94,95,95,95,95,95,96,97,97,98,97,98,97,98,97,98,97,98,97,98,97,97,97,97,97,97,97,97,97,97,97,96,96,96,95,96,96,96,95,95,94,95,93,94,94,94,93,95,93,93,93,93,91,90,91,91,91,86,89,86,87,84,82,86,82,81,78,73,76,74,71,75,76,78,69,77,71,71,71,78,75,81,80,70,82,67,74,79,79,76,76,82,76,74,78,85,76,77,79,84,80,77,85,81,83,83,81,80,82,82,81,79,81,81,80,80,81,82,79,78,78,81,75,77,77,80,77,77,78,73,77,80,74,69,75,77,72,72,70,76,74,71,72,74,76,77,73,79,74,80,79,76,80,78,78,76,78,79,81,78,77,83,82,83,83,80,82,82,85,86,87,87,85,85,85,89,86,86,88,88,85,84,86,88,86,85,86,91,86,89,87,88,87,88,87,87,89,87,88,87,88,89,90,89,86,90,89,88,87,88,88,88,85,86,89,87,85,88,85,88,87,88,88,89,88,89,89,90,89,90,91,92,91,91,91,92,90,91,93,92,93,93,94,94,94,95,93,94,94,95,95,95,95,96,96,96,96,96,96,96,96,97,96,97,96,97,97,97,97,97,97,97,97,97,97,98,97,98,97,98,97,98,97,97,97,98,97,98,97,98,97,98,97,98,97,98,97,98,97,98,97,98,98,98,98,98,94,95,94,95,94,95,95,94,95,94,95,97,95,96,96,97,96,96,95,95,95,95,95,94,94,95,93,93,93,94,93,94,95,93,93,91,91,92,91,90,88,85,86,86,85,84,83,82,77,84,80,83,77,77,79,77,83,78,78,76,83,80,79,77,77,77,75,76,71,77,79,74,75,78,77,81,77,78,77,81,78,81,82,80,82,82,83,83,81,84,79,77,83,81,82,84,80,85,81,78,76,82,79,78,79,83,79,77,80,77,79,80,76,74,77,80,82,73,72,76,76,75,79,76,81,78,79,81,76,83,81,78,79,79,80,80,80,77,77,83,82,84,83,80,82,83,81,80,84,82,84,86,85,87,86,86,89,89,85,88,90,90,86,87,86,87,89,88,87,86,86,88,89,89,88,88,87,90,85,89,89,87,89,91,88,88,87,88,90,88,87,88,87,89,87,88,88,88,90,89,89,90,87,89,90,89,87,90,89,91,88,91,89,92,90,91,91,93,91,92,92,93,92,92,93,94,95,93,94,94,94,95,94,95,94,96,95,96,96,96,95,96,96,97,96,97,96,97,97,97,96,98,97,97,96,97,97,98,97,97,97,98,97,98,97,98,97,98,97,98,97,94,93,95,94,96,95,96,95,96,95,96,95,98,97,97,97,98,97,97,97,97,97,97,97,97,97,97,97,96,96,96,96,96,95,96,95,96,95,95,95,94,93,94,92,94,94,93,93,94,93,93,93,92,89,89,89,90,88,86,87,86,85,84,82,84,76,80,80,75,82,76,80,82,78,77,77,81,81,79,79,75,68,80,77,75,76,81,78,69,76,72,73,76,77,77,74,74,79,83,79,78,80,81,83,82,85,80,82,83,81,84,81,80,84,81,82,81,81,78,75,77,81,72,81,78,76,75,74,76,74,77,73,74,82,75,75,69,72,69,70,70,76,75,78,80,76,74,74,78,77,79,78,77,72,78,76,84,77,79,81,81,80,78,83,82,81,79,78,83,86,80,85,84,84,85,85,87,84,86,84,88,87,85,84,85,88,89,86,87,85,88,88,85,87,88,88,87,84,88,86,88,86,88,87,86,84,86,88,87,86,86,88,91,88,88,86,90,88,86,90,87,87,88,90,90,88,89,89,90,90,88,89,90,91,90,91,92,92,94,94,94,93,94,93,94,94,94,94,94,95,95,95,96,95,95,96,96,96,97,95,97,96,97,97,97,96,97,97,97,97,97,97,97,97,97,97,97,97,98,97,98,97,97,97,97,97,98,97,98,97,98,97,97,97,98,95,94,94,96,95,96,95,95,95,96,95,96,95,98,97,98,97,98,97,97,96,97,96,96,96,96,96,95,95,95,95,95,95,95,94,94,94,93,93,94,94,93,93,92,91,91,92,90,91,91,91,88,88,85,83,81,82,81,82,79,76,79,82,75,78,79,77,78,78,79,78,75,72,78,72,76,75,77,73,71,76,77,78,78,72,78,79,76,74,80,79,75,83,80,80,80,83,83,83,82,85,81,84,83,81,79,79,83,79,82,82,81,77,76,79,77,79,78,77,74,72,74,83,80,76,79,76,72,74,76,73,73,77,72,72,75,73,81,81,80,79,83,78,77,82,80,81,79,81,79,83,83,78,79,80,85,86,84,88,85,84,84,81,86,87,87,87,88,88,86,86,88,87,86,89,89,87,87,88,89,88,87,88,88,87,86,88,88,84,86,86,85,87,85,86,88,88,86,87,89,86,89,90,90,87,89,89,89,89,89,88,89,88,89,88,90,91,90,90,91,90,92,92,90,92,91,92,91,91,94,92,93,92,94,92,94,94,94,94,93,95,95,95,95,95,95,96,95,96,96,96,96,96,97,96,97,97,97,97,97,97,97,97,97,97,97,97,97,98,98,98,97,98,98,98,98,97,98,98,98,98,95,94,93,93,96,96,96,96,96,96,96,97,98,98,98,98,98,98,98,97,97,98,98,97,98,98,97,98,97,97,97,97,97,97,96,97,96,96,96,95,95,94,95,95,95,95,94,94,95,94,94,93,93,92,93,91,91,92,91,91,91,89,88,89,89,88,88,88,87,88,85,88,86,88,88,88,84,83,83,87,85,86,85,85,85,88,84,83,82,83,85,84,86,87,86,87,88,86,88,91,88,88,89,88,87,88,88,89,88,88,89,90,89,88,89,89,86,89,86,86,87,87,87,87,86,85,88,90,87,84,85,87,91,85,87,88,83,84,88,85,83,86,85,88,86,89,88,87,85,86,86,83,86,86,90,85,86,87,86,85,89,87,89,86,88,87,88,87,88,90,90,90,91,91,90,91,90,90,90,90,91,90,89,92,91,91,91,91,90,92,89,90,91,87,91,92,91,90,90,90,89,90,91,92,90,70,77,99,70,84,82,87,92,92,90,89,92,87,90,89,93,90,91,92,92,91,92,93,92,91,92,94,92,91,92,93,94,94,93,94,95,95,93,93,94,95,95,94,96,95,95,95,95,95,96,96,96,96,97,96,97,97,96,97,97,97,97,97,97,97,97,97,97,97,97,97,97,98,98,98,97,98,98,98,98,98,98,98,98,98,98,98,97,97,97,97,97,96,98,98,98,98,98,98,98,98,98,98,97,98,98,97,98,97,97,97,97,97,97,97,97,97,97,96,96,97,96,96,96,95,95,95,95,95,95,95,94,94,94,95,93,92,94,94,93,92,92,90,91,93,91,90,91,90,89,90,84,89,88,88,88,88,87,85,85,86,86,87,89,87,88,84,89,83,87,86,86,88,91,89,89,88,84,88,88,89,86,89,88,87,88,90,88,89,90,91,90,88,91,87,88,89,89,89,88,88,88,88,88,90,88,87,88,89,87,89,88,88,87,86,88,88,88,86,89,89,86,87,86,86,89,89,87,84,89,80,88,87,88,90,87,87,89,87,87,89,88,89,90,89,89,88,90,88,87,88,89,91,89,88,90,89,89,90,90,89,90,93,90,89,90,89,91,90,91,90,92,90,90,89,90,89,91,89,90,88,90,91,90,89,91,91,90,91,91,90,90,89,89,89,90,86,90,91,87,92,91,91,91,91,91,90,92,92,92,91,91,91,91,93,93,93,93,93,93,93,94,95,95,95,94"
//    val dec=times.split(",").map(_.toDouble)
//
//    val smoothed = Smoothing1D.fourierSmooth(249,dec)
//    println(times)
//    println(smoothed.mkString(","))

//
//    val fts = DFT.dft(times.split(",").map(_.toDouble))
//    val apts = new Array[Double](fts.length)
//    val phase = new Array[Double](fts.length)
//    for(i<-0 until fts.length){
//      val re = fts(i).re()
//      val im = fts(i).im()
//      apts(i)=math.sqrt(re*re + im*im)
//      phase(i)=math.atan2(im,re)
//    }
//
//    println("原数据," + times)
//    println("振幅," + apts.mkString(","))
//    println("相位," + phase.mkString(","))
//
//    val k="0,4,10,20,50,100,144,200,287".split(",").map(_.toInt)
//    for(i<-0 until k.length){
//      var j = k(i)
//      val nfts=fts.zipWithIndex.map(x=>
//        if (x._2>j){
//          new Complex(0,0)
//        }else{
//          x._1
//        })
//      val ifts=DFT.idft(nfts).map(x=>x.re())
//      println("k=" + j + "," + ifts.mkString(","))
//    }



//    val trainFile      ="C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_FT_L.csv"
//    val testFile       ="C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_FT_P.csv"
//    val tgtVar         ="IDLE_CPU_AUTO"
//    val hostnode       ="ASCECUP01"
//    val corrThreshold    =0.6
//    val corrStep = 0.1
//    val clusters       =6
//    val maxIter        =1000
//    val tolerance      =0.001
//    val seed           =54321.toLong
//    val betaMin        =1.0
//    val betaStep       =0.0
//    val betaMax        =1.0
//    val randomCnt      =3
//    val maerThreshold  =0.023
//    val lastDmCnt      =1000
//
//    val list = GMMPredictor.autoCorrClustering(
//      trainFile      ,
//      testFile       ,
//      tgtVar         ,
//      hostnode       ,
//      corrThreshold    ,
//      corrStep,
//      clusters       ,
//      maxIter        ,
//      tolerance      ,
//      seed           ,
//      betaMin        ,
//      betaStep       ,
//      betaMax        ,
//      randomCnt      ,
//      maerThreshold  ,
//      lastDmCnt
//    )

//    val list = GMMPredictor.autoClustering(
//      trainFile      ,
//      testFile       ,
//      tgtVar         ,
//      hostnode       ,
//      corrThreshold    ,
//      clusters       ,
//      maxIter        ,
//      tolerance      ,
//      seed           ,
//      betaMin        ,
//      betaStep       ,
//      betaMax        ,
//      randomCnt      ,
//      maerThreshold  ,
//      lastDmCnt
//    )
//    println(list.toString)

    //    val st = System.currentTimeMillis()
//    for(i<-0 until 1000){
//      new StandardScalerExt()
//    }
//    val ed = System.currentTimeMillis()
//    println("耗时：" + (ed-st) + "ms")

    //计算百分位
//    val inpath = "C:\\Users\\wisdom\\Desktop\\data\\nums.csv"
//    val fdata = DataFrameUtil.readCsv(inpath)
//    val rddArr = DataFrameUtil.dfToRDDofArray(fdata)
//    val nums = rddArr.map(x=>x.last)
//    nums.take(10).foreach(println)
//    val sorted = nums.sortBy(x => -x)
//    val count = sorted.count()
//    val tailPercnt = 0.1
//    val tailCnt = math.floor(tailPercnt*count)
//    val index = sorted.zipWithIndex()
//    index.filter(x=>x._2<tailCnt).foreach(println)

    //会不会打印?
    //    for(i<- 1 to 1){
    //      println("8888888888888")
    //    }


    //    val inpath = "C:\\Users\\wisdom\\Desktop\\data\\ASCECUP01_p24_FT_L.csv"
    //    val fdata=DataFrameUtil.readCsv(inpath)
    //
    //    val mathDesc = new DescriptiveStatistics
    //    mathDesc.getPercentile(25)
    //
    //    val percnt = new Percentile()
    //
    //    val tmp = percnt.evaluate(fdata.select("Idle_CPU").collect().map(x=>x.getDouble(0)),50.0)
    //    println(tmp)

    //    val tmp = 2.345
    //    val format = "0.00"
    //    val df = new DecimalFormat(format)
    //    println(df.format(tmp))

    //    var line = "PID     : 14614742             TID : 1              PROC : db2diag"
    //    line = line.replaceAll("""\s*:\s*""",":")
    //    println(line)

    //    val timeRegx = """(\d{4}[-/\.]\d{2}[-/\.]\d{2})|((\d{2}[-/\.]\d{2}[-/\.]\d{4}))""".r
    //    val timeRegx = """\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6}""".r
    //    val testStr = "2015-07-16-16.41.08.570656+480 I1A1535              LEVEL: Event"
    //    val tmp = timeRegx.findFirstIn(testStr)
    //    println(tmp.isEmpty)

    //    val tmpStr = "EDUID   : 4410                 EDUNAME: db2logmgr (CAOPDB) 0 FUNCTION: DB2 UDB, data protection services, sqlpgArchiveLogFile, probe:3180 DATA #1 : <preformatted>"
    //    tmpStr.split("""\W""").filter(x=>x.length>0).foreach(println)


    //    import ContextUtil.sqlContext.implicits._
    //
    //    val clean_log = sc.textFile("C:\\Users\\wisdom\\Desktop\\clean_data.txt")
    //    val input = clean_log.map(_.split("""\|\|""")).map(x => log_sentence(x(2).replaceAll("""\(|\)|\:|/"""," "), x(0))).toDF()
    //    val tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("raw").setPattern("\\s+")
    //    val wordsData = tokenizer.transform(input)
    //    // remove stop words
    //    val modi_input = new StopWordsRemover()
    //      .setInputCol("raw")
    //      .setOutputCol("words")
    //      .transform(wordsData)
    //    // Using CountVectorizer to avoid collisions
    //    // can also set minTF/minDF
    //    val tfModel = new CountVectorizer()
    //      .setInputCol("words")
    //      .setOutputCol("TFvalues")
    //      .setMinDF(5)
    //      .fit(modi_input)
    //    val vocab = tfModel.vocabulary
    //    val tfData = tfModel.transform(modi_input)
    //    val idf = new IDF().setInputCol("TFvalues").setOutputCol("TFIDFvalues")
    //    val idfModel = idf.fit(tfData)
    //    val idfData = idfModel.transform(tfData)
    //    idfData.printSchema()
    //    idfData.select("TFvalues").take(10).foreach(println)
    //
    //    val sentenceData = ContextUtil.sqlContext.createDataFrame(Seq(
    //      (0, "Hi I heard about Spark"),
    //      (0, "I wish Java could use case classes"),
    //      (1, "Logistic regression models are neat")
    //    )).toDF("label", "sentence")
    //
    //    val tokenizer1 = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    //    val wordsData1 = tokenizer1.transform(sentenceData)
    //    val hashingTF = new HashingTF()
    //      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    //    val featurizedData = hashingTF.transform(wordsData1)
    //    val idf1 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //    val idfModel1 = idf1.fit(featurizedData)
    //    val rescaledData = idfModel1.transform(featurizedData)
    //    rescaledData.select("features", "label").take(3).foreach(println)


    //    val f = "C:\\Users\\wisdom\\Desktop\\entropy.csv"
    //    val df = DataFrameUtil.readCsv(f)
    //    df.describe().show()
    //    val cols = df.columns
    //    val exprCol = new Array[String](cols.length*2)
    //    for(i<-0 until cols.length){
    //      exprCol(2*i) = "max(" + cols(i) + ")"
    //      exprCol(2*i + 1) = "min(" + cols(i) + ")"
    //    }
    //    df.selectExpr(exprCol: _*).show()
    //    val expr = df.selectExpr(exprCol: _*).first()
    //    val maxium = new Array[Double](cols.length)
    //    val minimum = new Array[Double](cols.length)
    //    for(i<-0 until cols.length){
    //      maxium(i) = expr.getDouble(2*i)
    //      minimum(i) = expr.getDouble(2*i + 1)
    //    }
    //    println(maxium.mkString(","))
    //    println(minimum.mkString(","))

    //    val tgt = "IDLE_CPU_5"
    //    println(tgt.substring(0,tgt.lastIndexOf("_")))
    //    println(tgt.substring(tgt.lastIndexOf("_")+1))

    //    val f = "C:\\Users\\wisdom\\Desktop\\entropy.csv"
    //    val df = DataFrameUtil.readCsv(f)
    //    val tmp = df.groupBy("x_square","x").count()
    //    tmp.printSchema()
    //    tmp.show()

    //    val stopwords = new StopWordsRemover()

    //    val tmp1 = scala.collection.mutable.Map[String,Int]()
    //    val tmp2 = scala.collection.mutable.Map[String,Int]()
    //
    //    tmp1.put("a",1)
    //    tmp1.put("b",1)
    //
    //    tmp2.put("a",2)
    //    tmp2.put("c",2)
    //
    //    println((tmp1 ++ tmp2).mkString(","))

    //    println(0*math.log(0))

    //    val realArr = new Array[Double](200)
    //    realArr(0)= -1.0
    //    for(i<-1 until realArr.length){
    //      realArr(i)=realArr(i-1)+0.01
    //    }
    //    val spcnt = 5
    //    val interval = (realArr.max-realArr.min)/(spcnt-1)
    //    val splits = new Array[Double](spcnt)
    //    splits(0)=realArr.min
    //    for(i<-1 until spcnt-1){
    //      splits(i)=splits(i-1)+interval
    //    }
    //
    //    splits(spcnt-1) = Double.PositiveInfinity
    //
    //    val dataFrame = ContextUtil.sqlContext.createDataFrame(realArr.map(Tuple1.apply)).toDF("features")
    //
    //
    //    val bucketizer = new Bucketizer()
    //      .setInputCol("features")
    //      .setOutputCol("bucketedFeatures")
    //      .setSplits(splits)
    //
    //    val bucketedData = bucketizer.transform(dataFrame)
    //    bucketedData.printSchema()
    //    bucketedData.show(200)
    //
    //    println(splits.mkString(","))


    //    println(System.getProperties)


    //    val modelPath = MLPathUtil.gmmModelPath("JVM_MEM_ft24","ASCECUP09")
    //    val model = new MLlibGaussianMixture().getGMM(modelPath)
    //    for(i<-0 until model.weights.length){
    //      println("Gaussian_" + i + " weights:" + model.weights(i) + " mu:" + model.gaussians(i).mu.toArray.mkString(","))
    //    }
    //
    //    for(i<-0 until model.weights.length){
    //      print("Gaussian_" + i + " weights:" + model.weights(i) + " sigma:" )
    //      for(j<-0 until model.gaussians(i).mu.size){
    //        print(model.gaussians(i).sigma(j,j))
    //        print(",")
    //      }
    //      println()
    //    }

    //    val boostStrategy = BoostingStrategy.defaultParams("regression")
    //
    //    val gbt = new GradientBoostedTrees(boostStrategy)

    //    var tmp= -3.toDouble
    //    for(i<-0 until 120){
    //      val mu1 = 0
    //      val sigma1 = 1
    //      val y1 = 1.0/(math.sqrt(2*3.1415)*sigma1)*math.exp(-1*math.pow(tmp-mu1,2)/(2*math.pow(sigma1,2)))
    //
    //      val mu2 = -1
    //      val sigma2 = 1.8
    //      val y2 = 1.0/(math.sqrt(2*3.1415)*sigma2)*math.exp(-1*math.pow(tmp-mu2,2)/(2*math.pow(sigma2,2)))
    //
    //      val logy1 = math.log(y1)
    //      val expy1 = math.exp(y1/10)
    //
    //      println(tmp+","+y1+","+expy1)
    //      tmp += 0.05
    //    }

    //    val format="0"
    //    val df = new DecimalFormat(format)
    //    println(df.format(234.95))

    //    val in = "C:\\Users\\wisdom\\Desktop\\IDLE_CPU\\ASGTP02\\mid\\scaler\\data"
    //    val k = 50
    //    val outModel = "C:\\Users\\wisdom\\Desktop\\IDLE_CPU\\ASGTP02\\mid\\pca\\model"
    //    val outData = "C:\\Users\\wisdom\\Desktop\\pcaTest"
    //    val pca = new MLlibPCA()
    //    //验证PCA产生的数据与SVD的一致
    ////    pca.trainAndSaveWithPCA(in,k,outModel,outData)
    //    //验证SVD产生的特征向量是单位向量
    //    val pc = pca.getPCMatrix(outModel)
    //    val cnt = pc.numCols
    //    val cols = pc.toArray.grouped(pc.numRows)
    //    println(cols.map(x=>math.sqrt(x.map(y=>y*y).sum)).toArray.mkString(","))
    //    var ss=0.0
    //    for(j<-0 until pc.numRows){
    //      ss += pc(j,0)*pc(j,0)
    //    }
    //    println(ss)
    //    println(math.sqrt(ss))


    //    val scalerModelPath = MLPathUtil.scalerModelPath("IDLE_CPU","ASGTP02")
    //    val scalerModel = StandardScalerModel.load(scalerModelPath)
    //    val fileUri = "C:\\Users\\wisdom\\Desktop\\persitObj\\scaler.obj"
    //    PersitObjectUtil.writeObjectToFile(scalerModel,fileUri)
    //    val sm = PersitObjectUtil.readObjectFromFile(fileUri).asInstanceOf[StandardScalerModel]
    //    println(sm.mean)
    //    println(sm.std)

    //    val inPath="C:\\Users\\wisdom\\Desktop\\IDLE_CPU\\ASGTP02\\mid\\scaler\\data"
    //    val fdata = DataFrameUtil.readCsv(inPath)
    //    val feat = DataFrameUtil.dfToDfOfLabelFeature(fdata)
    //    val pca = new PCA()
    //      .setInputCol("features")
    //      .setOutputCol("pcaFeatures")
    //      .setK(66)
    //      .fit(feat)
    //    val pcaDF = pca.transform(feat)
    //
    //    pcaDF.select("pcaFeatures").take(200).foreach(x=>println(x.toString()))

    //    val timeformat="0.0000"
    //    val df = new DecimalFormat(timeformat);
    //    println(df.format(1234.7043308344543946))

    //    val mlc = new mlpc()
    //    println(mlc.explainParams())

    //    val inputFeatures = 0
    //    val outputClasses = 4
    //    val intermediate = "1,2,3".split(",").map(x=>x.toInt)
    //    val layers = Array(inputFeatures) ++ intermediate :+ outputClasses
    //    println(layers.mkString(","))

    //    val timeformat="00"
    //    val df = new DecimalFormat(timeformat);
    //    val num= 2.0
    //    println(df.format(num))

    //    PropertyUtil.saveModelProperty("testkey","testvalue")

    //    val tmp = "2.0,14.0,11.0"
    //      .split(",")
    //      .map(x=>x.toString.toDouble):+0.0
    //    for (i<- 0 until tmp.length) {
    //      val tmp1 = tmp(i)/2
    //      println(i + ":" + tmp1)
    //    }

    //    val tmp1 = Calendar.getInstance()
    //    println(tmp1.getTime)
    //    val tmp2 = Calendar.getInstance()
    //    tmp2.set(Calendar.DAY_OF_MONTH,18)
    //    tmp2.set(Calendar.HOUR_OF_DAY,13)
    //    tmp2.set(Calendar.MINUTE,41)
    //    println(tmp2.getTime)


  }

  /**
    * 从xml中解析“字段-值”
    *
    * @param xmlStr xml字符串
    * @return key-value Map
    */
  def extractCol(xmlStr: String): util.HashMap[String,String] = {
    val kv = new util.HashMap[String,String]()
    val rgx = "<([a-z]:)(\\w+)>([^<]*)</\\1\\2>"
    val it = rgx.r.findAllIn(xmlStr)
    while(it.hasNext){
      it.next()
      val key=it.group(2)
      val value=it.group(3)
      println(key + ":" + value)
      kv.put(key,value)
    }
    kv
  }
}
