package com.wisdom.spark.etl.DataProcessing


import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.etl.ppn.{Complex, DFT}
import com.wisdom.spark.etl.util.{FFTUtil, InitUtil}
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
import scala.util.control.Breaks

/**
  * Created by wisdom on 2016/11/11.
  * 需要注意的
  * 1 offlineDataProcessing中arr的长度根据傅里叶变换需求，设置不同长度，例OPMDB表最低学习一周的长度为60*24*7，而
  * System 12*24*7,需要改变，现在只选取了288*7，对于OPMDB来说是1.4天作为一个周期，对于System来说，一周为一个周期
  */
object OfflineDataProcessing {
  @transient
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val properties = ItoaPropertyUtil.getProperties(args(0))
    val map = new ConcurrentHashMap[String, AllPredcictTarget] {}
    val init = new InitUtil(properties, map)
    val sc = SparkContextUtil.getInstance()
    //    val files=sc.textFile(init.commonInputPath+"*")
    //get all files under specific directories
    val files = sc.wholeTextFiles(init.commonInputPath + "*")
    files.foreach(x => offlineDataProcessing(init, x._1.substring(x._1.lastIndexOf("/") + 1, x._1.indexOf(".")), x._2))

    sc.stop()
  }

  /**
    * 过滤主机->去重->按时间排序
    *
    * @param filenameTemp 文件名
    * @param content      整个文档内容
    */
  def offlineDataProcessing(init: InitUtil, filenameTemp: String, content: String): Unit = {
    val filename = filenameTemp.toUpperCase()
//    if (init.targetMap.contains(init.currentTableName + "_" + filename)) {
      val arr = content.split("\n")
      //TODO
      val unique_arr = arr.distinct
      Sorting.quickSort(unique_arr)
      val sortedStr = unique_arr.mkString("\n")
      iterateEachFile(init, filename, sortedStr)
//    } else {
//      logger.error("*************************offlineDataProcessing:不包含该主机" + filename + "*************************")
//    }

  }

  /**
    * 遍历文件的每一行，对其进行处理
    *
    * @param filename 文件名
    * @param y        整个文档内容
    */
  def iterateEachFile(init: InitUtil, filename: String, y: String): Unit = {
    //.map(x=>x.replace("\r","")),need to remove \r if read windows files
    println(filename + "------")
    //去掉时间戳字段，方便计算
    val content = init.eachStripTimestamp(y)
    //减去16，表示4(前4个时刻)+12（后12个时刻）
    val contentLength = content.length - 16
    val column_current_3 = content.take(contentLength)
    val column_current_2 = content.tail.take(contentLength)
    val column_current_1 = content.tail.tail.take(contentLength)
    val column_current = content.tail.tail.tail.take(contentLength)
    val column_current_withTimestamp = y.split("\n").tail.tail.tail.take(contentLength)
    //当前时刻0减去上一时刻1数据数组...
    var c01 = new Array[String](contentLength * 2)
    //上一时刻1减去上两时刻2数据数组...
    var c12 = new Array[String](contentLength * 2)
    var c23 = new Array[String](contentLength * 2)
    try {
      c01 = init.minus(column_current, column_current_1)
      c12 = init.minus(column_current_1, column_current_2)
      c23 = init.minus(column_current_2, column_current_3)
    } catch {
      case e: Exception => logger.error("**********" + filename + "com.wisdom.spark.DataProocessing.OfflineDataProcessing.iterateEachFile ERROR：减法异常" + "**********")
    }
    //正常数据范围，小于smallerRule或大于largerRule为异常数据
    val smallerRule = init.currentBean.getPredictionFieldFilterRule.split(",")(0).toDouble
    val largerRule = init.currentBean.getPredictionFieldFilterRule.split(",")(1).toDouble

    //当前时刻预测字段值
    val allPredictionFieldValue = y.split("\n").map(_.split(",")(init.currentBean.getPredictionFieldIndex))
    //傅里叶变换
    val dftModel = getDFTModel2(init, column_current_withTimestamp)
    if (init.OutputDFTModel) {
      saveDFTModel(init, filename, dftModel)
    }
    //未来时刻预测字段值
    var allPredictionFieldValueFuture = allPredictionFieldValue.tail.tail.tail
    for (i <- 1 until 13) {
      //未来时刻+1预测字段值，未来时刻+3预测字段值...
      allPredictionFieldValueFuture = allPredictionFieldValueFuture.tail
      if (i == 1 || i == 3 || i == 6 || i == 12) {
        val allPredictionField_0 = allPredictionFieldValueFuture.take(contentLength)
        //所有数据索引
        val allDataIndexBuffer = new ArrayBuffer[Int]()
        //正常数据索引
        val normalDataIndexBuffer = new ArrayBuffer[Int]()
        //异常数据索引
        val abnormalDataIndexBuffer = new ArrayBuffer[Int]()
        for (i <- allPredictionField_0.indices) {
          /*
          //小于smallerRule或大于largerRule为异常数据
          if (allPredictionField_0(i).toDouble <= smallerRule || allPredictionField_0(i).toDouble >= largerRule) {
            abnormalDataIndexBuffer.append(i)
          } else {
            normalDataIndexBuffer.append(i)
          }
          */
          allDataIndexBuffer.append(i)
        }
        val allDataIndex = allDataIndexBuffer.toArray
        val normalDataIndex = normalDataIndexBuffer.toArray
        val abnormalDataIndex = abnormalDataIndexBuffer.toArray
        //暂时未使用
        if (init.OutputDataAll) {
          mergeAndSaveData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, allDataIndex, filename, "_" + i)
        }
        //暂时未使用
        if (init.OutputDataPart) {
          mergeAndSaveData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, normalDataIndex, filename, "_" + i + "_N")
          mergeAndSaveData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, abnormalDataIndex, filename, "_" + i + "_A")
        }

//        val allDataArray = mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, i)
        //所有数据，每行
        val allDataArray = mergeData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, allDataIndex,i)
        /*


        //正常数据
        val normalDataArray = mergeData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, normalDataIndex,i)
        //异常数据
        val abnormalDataArray = mergeData(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, dftModel, allPredictionField_0, abnormalDataIndex,i)
        //正常数据与异常数据比率
        val ratio = init.ETL_ratio
        //需要多少条数据
        val allLength = init.trainFileLength
        //正常数据条数
        val normalLength = (allLength * ratio).toInt
        //异常数据条数
        val abnormalLength = (allLength * (1 - ratio)).toInt

        val arrayBuffer = new ArrayBuffer[String]()
        if (normalDataIndex.length != 0) {
          //正常数据条数多于需要的条数，则直接take
          if (normalDataIndex.length > normalLength) {
            normalDataArray.take(normalLength).map(arrayBuffer.append(_))
          } else {
            //正常数据条数少于需要的正常数据条数，重复正常数据
            val loopCount = normalLength / normalDataIndex.length
            val loopModule = normalLength % normalDataIndex.length
            for (i <- 0 until loopCount) {
              normalDataArray.map(arrayBuffer.append(_))
            }
            normalDataArray.take(loopModule).map(arrayBuffer.append(_))
          }
        }

        if (abnormalDataIndex.length != 0) {
          if (abnormalDataIndex.length > abnormalLength) {
            abnormalDataArray.take(abnormalLength).map(arrayBuffer.append(_))
          } else {
            val loopCount = abnormalLength / abnormalDataIndex.length
            val loopModule = abnormalLength % abnormalDataIndex.length
            for (i <- 0 until loopCount) {
              abnormalDataArray.map(arrayBuffer.append(_))
            }
            abnormalDataArray.take(loopModule).map(arrayBuffer.append(_))
          }
        }
        val finalFileArray = arrayBuffer.toArray
        Sorting.quickSort(finalFileArray)
        */

        //根据配置的时间，保存训练文件
        saveTrainAndTestFile(init, allDataArray, filename, "_" + i + "_TRAIN", init.trainFileStartTime, init.trainFileEndTime)
        //根据配置的时间，保存测试文件，一般取最后3天
        saveTrainAndTestFile(init, allDataArray, filename, "_" + i + "_TEST", init.testFileStartTime, init.testFileEndTime)

      }


    }


    //    val allPredictionField_0 = allPredictionFieldValueFuture.tail.take(column_current_3.length)
    //    val allPredictionField_0_ft = saveDFTModel(init, filename,allPredictionField_0)
    //    val allDataIndexBuffer = new ArrayBuffer[Int]()
    //    val normalDataIndexBuffer = new ArrayBuffer[Int]()
    //    val abnormalDataIndexBuffer = new ArrayBuffer[Int]()
    //    for (i <- allPredictionField_0.indices) {
    //      if (allPredictionField_0(i).toDouble <= init.currentBean.getPredictionFieldFilterRule) {
    //        normalDataIndexBuffer.append(i)
    //      } else {
    //        abnormalDataIndexBuffer.append(i)
    //      }
    //      allDataIndexBuffer.append(i)
    //    }
    //
    //    val normalDataIndex = normalDataIndexBuffer.toArray
    //    val abnormalDataIndex  = abnormalDataIndexBuffer.toArray
    ////    if(normalDataIndex.length==0||abnormalDataIndex==0){
    ////
    ////    }
    //
    //
    //    if (init.OutputDataAll) {
    //      mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, allDataIndexBuffer.toArray, filename, "_0")
    //    }
    //    if (init.OutputDataPart) {
    //      mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, normalDataIndex, filename, "_0_N")
    //      mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, abnormalDataIndex, filename, "_0_A")
    //    }
    //    val allDataArray=mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, allDataIndexBuffer.toArray)
    //    val normalDataArray = mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, normalDataIndex)
    //    val abnormalDataArray = mergeArr(init, column_current_withTimestamp, column_current_1, column_current_2, column_current_3, c01, c12, c23, allPredictionField_0_ft, allPredictionField_0, abnormalDataIndex)
    //
    //    val ratio = init.ETL_ratio
    //    val allLength = init.trainFileLength
    //    val normalLength = (allLength * ratio).toInt
    //    val abnormalLength = (allLength * (1-ratio)).toInt
    //
    //    val arrayBuffer = new ArrayBuffer[String]()
    //    if(normalDataIndex.length!=0){
    //      if (normalDataIndex.length > normalLength) {
    //        normalDataArray.take(normalLength).map(arrayBuffer.append(_))
    //      } else {
    //        val loopCount = normalLength / normalDataIndex.length
    //        val loopModule = normalLength % normalDataIndex.length
    //        for (i <- 0 until loopCount) {
    //          normalDataArray.map(arrayBuffer.append(_))
    //        }
    //        normalDataArray.take(loopModule).map(arrayBuffer.append(_))
    //      }
    //    }
    //
    //    if(abnormalDataIndex.length!=0){
    //      if (abnormalDataIndex.length > abnormalLength) {
    //        abnormalDataArray.take(abnormalLength).map(arrayBuffer.append(_))
    //      } else {
    //        val loopCount = abnormalLength / abnormalDataIndex.length
    //        val loopModule = abnormalLength % abnormalDataIndex.length
    //        for (i <- 0 until loopCount) {
    //          abnormalDataArray.map(arrayBuffer.append(_))
    //        }
    //        abnormalDataArray.take(loopModule).map(arrayBuffer.append(_))
    //      }
    //    }
    //    val finalFileArray=arrayBuffer.toArray
    //    Sorting.quickSort(finalFileArray)
    //
    //
    //    saveTrainAndTestFile(init,finalFileArray,filename,"_0_train",0)
    //    saveTrainAndTestFile(init,allDataArray,filename,"_0_test",init.testFileStartTime)

    //    if (init.OutputDFTModel) {
    //      val len = column_current_withTimestamp.length
    //      if (len > 288 * 7) {
    //        val predictionFields = init.currentBean.getPredictionFields
    //        val predictionFieldsArr = predictionFields.split(",")
    //
    //        for (i <- 0 until predictionFieldsArr.length) {
    //          saveDFTModel(init, filename, column_current_withTimestamp, predictionFieldsArr(i))
    //        }
    //      } else {
    //        logger.error("*************************offlineDataProcessing:" + "长度小于2016，不足一个傅里叶变换周期" + "*************************")
    //      }
    //    }

    return

  }

  /**
    * 获取DFT模型
    * @param init
    * @param hostname
    * @param currentPredictionFieldValueString
    * @return
    */
  def getDFTModel(init: InitUtil, hostname: String, currentPredictionFieldValueString: Array[String]): Array[Double] = {
    val currentPredictionFieldValueDouble = currentPredictionFieldValueString.map(_.toDouble)
    //正变换
    val fftModels = DFT.dft(currentPredictionFieldValueDouble)
    val complex = new Complex(0.0, 0.0)
    //
    for (i <- fftModels.indices) {
      if (i > init.DFTModelFilter) {
        fftModels.update(i, complex)
      }
    }
    val i_fftModels = DFT.idft(fftModels)
    val arrayDouble = new Array[Double](2016)

    for (i <- i_fftModels.indices) {
      if (i < 2016) {
        arrayDouble(i) = i_fftModels(i).re()
      }
    }
    arrayDouble

  }

  /**
    * 保存傅里叶变换模型
    * @param init
    * @param hostname
    * @param arr
    */
  def saveDFTModel(init: InitUtil, hostname: String, arr: Array[Double]): Unit = {
    val filePath: String = init.DFTModelRootPath + init.currentTableName + "/" + hostname + "/" + init.currentBean.getPredictionField + "/" + hostname + ".txt"
    val path = new Path(filePath)
    val conf = new Configuration()
    val fs = FileSystem.newInstance(conf)
    val os = fs.create(path)
    //傅里叶变换模型数组
    for (i <- arr.indices) {
      os.write((arr(i) + "\n").getBytes())
    }
    os.close()
    fs.close()
  }

  /**
    * 获得傅里叶变换模型
    * @param init
    * @param column_current_withTimestamp
    * @return
    */
  def getDFTModel(init: InitUtil, column_current_withTimestamp: Array[String]): Array[Double] = {
    val arr = new Array[Double](288 * 7)

    val breaks = new Breaks
    breaks.breakable(
      for (i <- column_current_withTimestamp.indices) {
        val row = column_current_withTimestamp(i)
        var cols = row.split(",")(0)
        val dataDate = row.split(",")(0).toLong

        val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
        val date = simpleDateFormatStd.parse(cols)
        val simpleDateFormatTemp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
        cols = simpleDateFormatTemp.format(date)
        //获取大于设置的时间之后的数据
        if (init.FourierModelStartTime < dataDate) {
          val timestamp = cols.split("-")
          //
          val firstPoint = timestamp(0) + timestamp(1) + timestamp(2) + timestamp(3) + timestamp(4) + timestamp(5)
          val firstPointMin = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "00" + "00"
          val firstPointMax = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "04" + "00"
          val isFirstPoint = FFTUtil.isFistOrLastPoint(firstPointMin, firstPoint, firstPointMax)
          if (isFirstPoint) {
            val lastPoint = column_current_withTimestamp(i + 287)


          }



          //          val isFirstPoint = FFTUtil.selectOneWeek(firstPointMin, firstPoint, firstPointMax)
          //timeStamp is sunday
          if (isFirstPoint) {
            var j = i + 1
            var k = 1

            val predictionFieldIndex = init.currentBean.getPredictionFieldIndex
            //          logger.error("**********init.currentBean.getPredictionFieldsMap.containsKey"+init.currentBean.getPredictionFieldsMap.containsKey(predictionField))
            //          logger.error("**********init.currentBean.getPredictionFieldsMap"+init.currentBean.getPredictionFieldsMap)
            //          logger.error("********************predictionFieldIndex"+predictionField+"********************")
            //          logger.error("********************column_current_withTimestamp(i)"+column_current_withTimestamp(i)+"********************")
            //          logger.error("********************predictionFieldIndex"+predictionFieldIndex+"********************")
            val actual = column_current_withTimestamp(i).split(",")(predictionFieldIndex)
            arr(0) = actual.toDouble

            while (j < column_current_withTimestamp.length && j < i + 288 * 7) {
              val previousData = column_current_withTimestamp(j - 1)
              val currentData = column_current_withTimestamp(j)
              var data = previousData
              if (FFTUtil.dataIsContinuous(previousData, currentData)) {
                data = currentData
              } else {
                data = previousData
              }
              val actual = data.split(",")(predictionFieldIndex)
              arr(k) = actual.toDouble
              j += 1
              k += 1
            }
            breaks.break()
          }
        }

      }
    )
    val fftModels = DFT.dft(arr)
    val complex = new Complex(0.0, 0.0)
    for (i <- fftModels.indices) {
      if (i > init.DFTModelFilter) {
        fftModels.update(i, complex)
      }
    }
    val i_fftModels = DFT.idft(fftModels)

    val dft_arr = new Array[Double](288 * 7)

    for (i <- i_fftModels.indices) {
      val i_fftModel = i_fftModels(i).re()
      dft_arr(i) = i_fftModel
    }

    dft_arr
  }


  def getDFTModel2(init: InitUtil, column_current_withTimestamp: Array[String]): Array[Double] = {
    //一周的连续数据
    val oneWeekData = init.selectOneWeekData(init.currentBean.getPredictionFieldIndex, column_current_withTimestamp)
    //傅里叶正变换
    val fftModels = DFT.dft(oneWeekData)
    val complex = new Complex(0.0, 0.0)
    for (i <- fftModels.indices) {
      if (i < init.DFTModelFilter ) {
        if(i!=0){
          val complex2=new Complex(fftModels(i).re()*init.DFTMODELMULTI,fftModels(i).im()*init.DFTMODELMULTI)
          fftModels.update(i, complex2)
        }

      }else{
        fftModels.update(i, complex)
      }
    }
    val i_fftModels = DFT.idft(fftModels)

    val dft_arr = new Array[Double](288 * 7)

    for (i <- i_fftModels.indices) {
      val i_fftModel = i_fftModels(i).re()
      dft_arr(i) = i_fftModel
    }

    dft_arr

  }


  /**
    * 207-209,5-4,6-8...
    *
    * @param a is Array[String],like Array((207,5,6,89,0,2)(208,2,1,96,0,4))
    * @param b is Array[String],like Array((209,4,8,11,0,2)(210,5,4,96,0,4))
    * @return Array[String]
    */
  def minus(a: Array[String], b: Array[String]): Array[String] = {
    val c: Array[String] = new Array[String](a.length)

    for (i <- a.indices) {
      //println("a-b="+a(i).toDouble+"-"+b(i).toDouble)
      val tempA: Array[String] = a(i).split(",")
      val tempB: Array[String] = b(i).split(",")
      val sb: StringBuilder = new StringBuilder
      for (j <- tempA.indices) {
        var res = ""
        try {
          res = (tempA(j).toDouble - tempB(j).toDouble).toString
        } catch {
          case e: Exception =>
            res = "0"
        }
        sb.append(res)
        if (j <= tempA.length - 2) {
          sb.append(",")
        }
      }

      c(i) = sb.toString()
    }

    c
  }

  /**
    * 蒋处理后的数据保存到指定路径
    *
    * @param column_current   当前时刻
    * @param column_current_1 当前时刻的上一时刻
    * @param column_current_2 ...
    * @param column_current_3 ...
    * @param c01              当前时刻减去上一时刻
    * @param c12              上一时刻减去上一时刻的上一时刻
    * @param c23              ...
    * @param filename         文件名
    */
  def mergeAndSaveData(init: InitUtil, column_current: Array[String], column_current_1: Array[String], column_current_2: Array[String], column_current_3: Array[String], c01: Array[String], c12: Array[String], c23: Array[String], ft: Array[Double], p: Array[String], dataIndex: Array[Int], filename: String, suffix: String): Unit = {
    val filePath: String = init.commonOutputPath + init.currentTableName + "/" + filename + "/" + init.currentBean.getPredictionField + "/" + filename + suffix + ".csv"
    val path = new Path(filePath)
    val conf = new Configuration()
    //    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)

    val os = fs.create(path)
    //csv文件的表头
    val sb = new StringBuffer()
    sb.append("Year,Month,Day,Hour,Minute,".toUpperCase())
    val neededHeads = init.neededHeads
    val neededFields = init.currentBean.getNeededFields
    for (i <- neededHeads.indices) {
      for (j <- 1 until neededFields.length) {
        sb.append(neededFields(j) + neededHeads(i) + ",")
      }

    }
    sb.append(init.currentBean.getPredictionField + "_FT" + ",").append(init.currentBean.getPredictionField)

    val columnHead = sb.toString
    val columnHeadLen = columnHead.split(",")
    val columnHeadLength = columnHeadLen.length

    //    logger.error("**********column_current.length**********"+column_current.length+"**********column_current_1.length**********"+column_current_1.length)
    //    logger.error("**********column_current_1.content**********"+column_current_1.mkString(","))
    //    println("**********column_current.length**********"+column_current.length+"**********column_current_1.length**********"+column_current_1.length)
    //    println("**********column_current_1.content**********"+column_current_1.mkString(","))
    os.writeBytes(columnHead + "\n")


    //    for(i <-column_current_p1.indices){
    //      val tempF:Array[String]=column_current_p1(i).split(",")
    //      //获取预测指标的下一时刻值
    //      val predictField:String=tempF(predictionFieldIndex)
    //
    //      os.writeBytes(column_current(i)+","+column_current_1(i)+","+column_current_2(i)+","+column_current_3(i)+","+c01(i)+","+c12(i)+","+c23(i)+","+predictField+"\n")
    //    }
    //    logger.error("column_current---" + column_current.length + "**********c01:" + c01.length + "------c23" + c23.length + "**********")

    for (k <- dataIndex.indices) {
      val i = dataIndex(k)
      var columnContent = "-1"
      try {
        val current = column_current(i).split(",")
        val MDHM = init.getGMMTimeStamp(current(0))
        val j = i % 2016
        current.update(0, MDHM)
        columnContent = current.mkString(",") + "," + column_current_1(i) + "," + column_current_2(i) + "," + column_current_3(i) + "," + c01(i) + "," + c12(i) + "," + c23(i) + "," + ft(j) + "," + p(i) + "\n"
      } catch {
        case e: Exception =>
          logger.error("column_current(i)---" + column_current(i) + "**********" + filename + ":ArrayOutOfBoundsException" + "**********" + e.printStackTrace())
      }
      val columnContentLen = columnContent.split(",")
      val columnContentLength = columnContentLen.length
      if (columnContentLength == columnHeadLength) {
        os.writeBytes(columnContent)
        os.flush()
      } else {
        logger.error("**********columnContent:" + columnContent)
        logger.error("**********columnContentLen:" + columnContentLength + "columnHeadLength" + columnHeadLength)
      }


    }
    os.close()
    fs.close()
  }

  def saveTrainAndTestFile(init: InitUtil, column_current: Array[String], filename: String, suffix: String, startTime: Long, endTime: Long): Unit = {
    val filePath: String = init.commonOutputPath + init.currentTableName + "/" + filename + "/" + init.currentBean.getPredictionField + "/" + filename + suffix + ".csv"
    val path = new Path(filePath)
    val conf = new Configuration()
    //    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)

    val os = fs.create(path)
    //csv文件的表头
    val sb = new StringBuffer()
    sb.append("Year,Month,Day,Hour,Minute,".toUpperCase())
    val neededHeads = init.neededHeads
    val neededFields = init.currentBean.getNeededFields
    for (i <- neededHeads.indices) {
      for (j <- 1 until neededFields.length) {
        sb.append(neededFields(j) + neededHeads(i) + ",")
      }

    }
    sb.append(init.currentBean.getPredictionField + "_FT" + ",").append(init.currentBean.getPredictionField)

    val columnHead = sb.toString
    val columnHeadLen = columnHead.split(",")
    val columnHeadLength = columnHeadLen.length

    os.writeBytes(columnHead + "\n")

    for (i <- column_current.indices) {
      val current = column_current(i).split(",")
      if (current(0).toLong > startTime && current(0).toLong < endTime) {
        var columnContent = "-1"
        try {
          val MDHM = init.getGMMTimeStamp(current(0))
          current.update(0, MDHM)
          columnContent = current.mkString(",") + "\n"
        } catch {
          case e: Exception =>
            logger.error("column_current(i)---" + column_current(i) + "**********" + filename + ":ArrayOutOfBoundsException" + "**********" + e.printStackTrace())
        }
        val columnContentLen = columnContent.split(",")
        val columnContentLength = columnContentLen.length
        if (columnContentLength == columnHeadLength) {
          os.writeBytes(columnContent)
          os.flush()
        } else {
          logger.error("**********columnContent:" + columnContent)
          logger.error("**********columnContentLen:" + columnContentLength + "columnHeadLength" + columnHeadLength)
        }
      }
    }
    os.close()
    fs.close()
  }

  def mergeData(init: InitUtil, column_current_withtimestamp: Array[String], column_current_1: Array[String], column_current_2: Array[String], column_current_3: Array[String], c01: Array[String], c12: Array[String], c23: Array[String], dftModel: Array[Double], p: Array[String], dataIndex: Array[Int],offset:Int): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]()
    for (k <- dataIndex.indices) {
      val i = dataIndex(k)
      var columnContent = "-1"
      try {
        val dateStr = column_current_withtimestamp(i).split(",")(0)
        val fftIndex = (init.getFFTIndex(dateStr) + offset)%2016
        columnContent = column_current_withtimestamp(i) + "," + column_current_1(i) + "," + column_current_2(i) + "," + column_current_3(i) + "," + c01(i) + "," + c12(i) + "," + c23(i) + "," + dftModel(fftIndex) + "," + p(i)
        arrayBuffer.append(columnContent)
      } catch {
        case e: Exception =>
          logger.error("column_current(i)---" + column_current_withtimestamp(i) + "**********" + ":ArrayOutOfBoundsException" + "**********" + e.printStackTrace())
      }
    }
    arrayBuffer.toArray
  }

  def mergeData(init: InitUtil, column_current_withtimestamp: Array[String], column_current_1: Array[String], column_current_2: Array[String], column_current_3: Array[String], c01: Array[String], c12: Array[String], c23: Array[String], dftModel: Array[Double], p: Array[String],offset: Int): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]()
    for (i <- column_current_withtimestamp.indices) {
      var columnContent = "-1"
      try {
        val dateStr = column_current_withtimestamp(i).split(",")(0)
        val fftIndex = init.getFFTIndex(dateStr) + offset
        columnContent = column_current_withtimestamp(i) + "," + column_current_1(i) + "," + column_current_2(i) + "," + column_current_3(i) + "," + c01(i) + "," + c12(i) + "," + c23(i) + "," + dftModel(fftIndex) + "," + p(i)
        arrayBuffer.append(columnContent)
      } catch {
        case e: Exception =>
          logger.error("column_current(i)---" + column_current_withtimestamp(i) + "**********" + ":ArrayOutOfBoundsException" + "**********" + e.printStackTrace())
      }
    }
    arrayBuffer.toArray
  }

}