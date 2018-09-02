package com.wisdom.spark.etl.util

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}
import java.util.{Calendar, Date, Properties}

import com.wisdom.spark.common.bean.PredResult
import com.wisdom.spark.common.util.SparkContextUtil
import com.wisdom.spark.etl.bean.{InitBean, ModelBean, SystemBean}
import com.wisdom.spark.etl.dao.MysqlDao
import com.wisdom.spark.etl.ppn.Complex
import com.wisdom.spark.ml.tgtVar.AllPredcictTarget
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.Logger
import spire.std.map

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.Breaks

/**
  * Created by htgeng on 2017/1/12.
  */
class InitUtil(props: Properties, modelTargetMap: ConcurrentHashMap[String, AllPredcictTarget]) extends Serializable {
  val modelMap = modelTargetMap

  var properties = props
  var previous = properties.getProperty("previous".toUpperCase()).toInt
  var future = properties.getProperty("future".toUpperCase())
  var commonInputPath = properties.getProperty("commonInputPath".toUpperCase())
  var commonOutputPath = properties.getProperty("commonOutputPath".toUpperCase())


  var FourierModelStartTime = properties.getProperty("FOURIERMODELSTARTTIME".toUpperCase()).toLong
  var FourierModelPrediction = properties.getProperty("FourierModelPrediction".toUpperCase())
  var DFTModelRootPath = properties.getProperty("DFTModelRootPath".toUpperCase())
  val OutputDFTModel = properties.getProperty("OutputDFTModel".toUpperCase()).toBoolean
  val OutputDataAll = properties.getProperty("OutputDataAll".toUpperCase()).toBoolean
  val OutputDataPart = properties.getProperty("OutputDataPart".toUpperCase()).toBoolean
  val DFTModelFilter = properties.getProperty("DFTModelFilter".toUpperCase()).toInt
  val DFTMODELMULTI = properties.getProperty("DFTMODELMULTI".toUpperCase()).toInt


  var currentTableName = properties.getProperty("currentTableName".toUpperCase()).toUpperCase()

  //  var neededFields=new util.ArrayList[String](0)
  var neededHeads = properties.getProperty("neededHeads".toUpperCase()).split(",")
  var allSystem = properties.getProperty("AllSystem".toUpperCase()).split(",")
  var allSystemTemp = properties.getProperty("AllSystemTemp".toUpperCase()).split(",")


  val predictionWaitTime = properties.getProperty("predictionWaitTime".toUpperCase()).toInt
  val relationWaitTime = properties.getProperty("relationWaitTime".toUpperCase()).toInt
  val trainFileLength = properties.getProperty("trainFileLength".toUpperCase()).toInt
  val ETL_ratio = properties.getProperty("ETL_ratio".toUpperCase()).toDouble
  val testFileStartTime = properties.getProperty("testFileStartTime".toUpperCase()).toLong
  val testFileEndTime = properties.getProperty("testFileEndTime".toUpperCase()).toLong
  val trainFileStartTime = properties.getProperty("trainFileStartTime".toUpperCase()).toLong
  val trainFileEndTime = properties.getProperty("trainFileEndTime".toUpperCase()).toLong

  val ETL_REGEX = properties.getProperty("ETL_REGEX".toUpperCase())

  val ETL_SQL=properties.getProperty("ETL_SQL".toUpperCase())


  //数据缓存队列
  private val list = MysqlDao.selectBean(ETL_SQL)
  val targetMap = new mutable.HashMap[String, ModelBean]()
  for (i <- 0 until list.size()) {
    val initBean = list.get(i)
    val modelBean = new ModelBean
    targetMap.put(initBean.getSystemName + "_" + initBean.getHostName, modelBean)
  }


  //存储要处理的主机信息,ip转换为主机名
  var OPMDBHostsMap = new util.HashMap[String, String]()
  var OPMDB_IP = properties.getProperty("OPMDB_IP".toUpperCase()).split(",")
  //要处理的主机所在平台
  var OPMDB_hosts = properties.getProperty("OPMDB_hosts".toUpperCase()).split(",")
  for (i <- OPMDB_IP.indices) {
    OPMDBHostsMap.put(OPMDB_IP(i).toUpperCase(), OPMDB_hosts(i))
  }


  //存储要处理的主机信息,ip转换为主机名
  var APPTRANSHostsMap = new util.HashMap[String, String]()
  var APPTRANS_IP = properties.getProperty("APPTRANS_IP".toUpperCase()).split(",")
  //要处理的主机所在平台
  var APPTRANS_hosts = properties.getProperty("APPTRANS_hosts".toUpperCase()).split(",")
  for (i <- APPTRANS_IP.indices) {
    APPTRANSHostsMap.put(APPTRANS_IP(i).toUpperCase(), APPTRANS_hosts(i))
  }

  //将所有表名的相关信息放入此map中
  val systemMap = new util.HashMap[String, SystemBean]()
  //离线处理用
  var currentBean = new SystemBean
  for (i <- 0 until allSystem.length) {
    val systemName = allSystem(i).toUpperCase()
    val fieldList = getFieldList(systemName)
    val systemBean = new SystemBean
    systemBean.setDelimiter(properties.getProperty(systemName + "_" + "delimiter".toUpperCase()))
    systemBean.setFieldList(fieldList)
    systemBean.setHostNameIndex(properties.getProperty(systemName + "_" + "hostNameIndex".toUpperCase()).toInt)
    systemBean.setHostNameInfo(properties.getProperty(systemName + "_" + "hostNameInfo".toUpperCase()).toInt)
    systemBean.setNeededFields(properties.getProperty(systemName + "_" + "neededFields".toUpperCase()).split(","))
    systemBean.setPredictionField(properties.getProperty(systemName + "_" + "predictionField".toUpperCase()))
    systemBean.setPredictionFields(properties.getProperty(systemName + "_" + "predictionFields".toUpperCase()))
    systemBean.setPredictionFieldsMap(getPredictionFieldsMap(systemName.toUpperCase(), properties.getProperty(systemName + "_" + "predictionFields".toUpperCase())))
    systemBean.setPredictionFieldIndex(getPredictionFieldIndex(systemName))
    systemBean.setPredictionFieldFilterRule(properties.getProperty(systemName + "_" + "predictionFieldFilterRule".toUpperCase()))
    systemBean.setSystemName(systemName)
    systemBean.setTimestampIndex(properties.getProperty(systemName + "_" + "timestampIndex".toUpperCase()))
    if (systemName.equalsIgnoreCase(currentTableName)) {
      currentBean = systemBean
    }
    systemMap.put(systemName, systemBean)

  }

  val DFTModelMap = loadDFTModel(DFTModelRootPath)


  /**
    * 获取所需表头索引
    *
    * @param tableName 表名
    * @return 所需表头索引信息
    */
  private def getFieldList(tableName: String): util.ArrayList[Int] = {
    //所需字段
    val neededFields = properties.getProperty(tableName + "_" + "neededFields".toUpperCase()).split(",")
    //所有字段
    val allFields = properties.getProperty(tableName + "_" + "allFields".toUpperCase()).split(",")
    //存取所需字段在所有字段中的位置
    val fieldIndexList = new util.ArrayList[Int]()

    val loopBreak = new Breaks

    for (i <- 0 until neededFields.length) {
      loopBreak.breakable(
        for (j <- 0 until allFields.length) {
          if (allFields(j).equalsIgnoreCase(neededFields(i))) {
            fieldIndexList.add(j)
            loopBreak.break()
          }
        }
      )
    }
    fieldIndexList
  }

  /**
    *
    * @param systemName
    * @param predictionFields
    * @return
    */
  private def getPredictionFieldsMap(systemName: String, predictionFields: String): util.HashMap[String, Int] = {
    val map = new util.HashMap[String, Int]()
    val predictionFieldsArr = predictionFields.split(",")
    for (i <- 0 until predictionFieldsArr.length) {
      val predictionField = predictionFieldsArr(i).toUpperCase()
      val predictionFieldIndex = getPredictionFieldIndex(systemName.toUpperCase(), predictionField)
      map.put(systemName.toUpperCase() + "_" + predictionField, predictionFieldIndex)
    }

    map
  }


  /** 离线处理使用
    * 获取指标所在位置
    *
    * @param tableName 表名
    * @return 指标所在位置
    */
  private def getPredictionFieldIndex(tableName: String): Int = {
    //所需字段
    val neededFields = properties.getProperty(tableName + "_" + "neededFields".toUpperCase()).split(",")
    //预测字段
    val predictionField = properties.getProperty(tableName + "_" + "predictionField".toUpperCase())
    //预测字段在所需字段中的位置
    var predictionFieldIndex: Int = -1
    val loopBreak = new Breaks
    loopBreak.breakable(
      for (j <- neededFields.indices) {
        if (predictionField.equalsIgnoreCase(neededFields(j))) {
          predictionFieldIndex = j
          loopBreak.break()
        }
      }
    )

    predictionFieldIndex
  }

  /** 实时处理使用
    * 获取指标所在位置
    *
    * @param tableName 表名
    * @return 指标所在位置
    */
  private def getPredictionFieldIndex(tableName: String, predictionField: String): Int = {
    //所需字段
    val neededFields = properties.getProperty(tableName + "_" + "neededFields".toUpperCase()).split(",")
    //预测字段在所需字段中的位置
    var predictionFieldIndex: Int = -1
    val loopBreak = new Breaks
    loopBreak.breakable(
      for (j <- neededFields.indices) {
        if (predictionField.equalsIgnoreCase(neededFields(j))) {
          predictionFieldIndex = j
          loopBreak.break()
        }
      }
    )

    predictionFieldIndex
  }

  def getTimeStamp(dateStrTemp: String): Long = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = simpleDateFormat.parse(dateStrTemp)
    date.getTime / 1000
  }


  //离线处理时使用
  /**
    * 统一各系统数据的时间戳格式
    * @param str 每条数据
    * @return
    */
  def dataFormat(str: String): String = {
    var formatString = ""
    try {
      if ("OPMDB".equalsIgnoreCase(currentTableName)) {
        val arr = str.split(",")
        val hostname = getOPMDBHostName(arr(0))
        val dateStr = getOPMDBTimeStamp(arr(2))
        arr.update(0, hostname)
        arr.update(2, dateStr)
        formatString = arr.mkString(",")
      } else if (currentTableName.toLowerCase().contains("reporter_status")) {
        val arr = str.split(",")
        val dateStr = getNetworkTimeStamp(arr(0))
        arr.update(0, dateStr)
        formatString = arr.mkString(",")
      } else if (currentTableName.equalsIgnoreCase("ecupin")||currentTableName.equalsIgnoreCase("ecupout")||currentTableName.equalsIgnoreCase("gaac")||currentTableName.equalsIgnoreCase("mobs")) {
        val arr = str.split(",")
        val hostname = getAPPTRANSHostName(arr(1))
        val dateStr = arr(2)
        val timeStr=arr(3)
        val dateTime=getTransTimeStamp(dateStr,timeStr)
        arr.update(1,hostname)
        arr.update(2, dateTime)
        arr.update(3, dateTime)
        formatString = arr.mkString(",")
      } else {
        val arr = str.split(",")
        val dateStr = getITMTimeStamp(arr(1))
        arr.update(1, dateStr)
        formatString = arr.mkString(",")
      }

      formatString
    } catch {
      case e: Exception =>
        //        logger.error("wrong data format")
        println("wrong data format")
        "NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR"
    }

  }

  //实时处理时使用
  def dataFormat(str: String, systemName: String, delimiter: String): String = {
    var formatString = ""
    try {
      if ("OPMDB".equalsIgnoreCase(systemName)) {
        val arr = str.split(delimiter)
        val hostname = getOPMDBHostName(arr(0))
        val dateStr = getOPMDBTimeStamp(arr(2))
        arr.update(0, hostname)
        arr.update(2, dateStr)
        formatString = arr.mkString("^")
      } else if (systemName.toLowerCase().contains("reporter_status")) {
        val arr = str.split(delimiter)
        val dateStr = getNetworkTimeStamp(arr(0))
        arr.update(0, dateStr)
        formatString = arr.mkString(",")
      } else if (systemName.toLowerCase().contains("ecupin")||systemName.toLowerCase().contains("ecupout")||systemName.toLowerCase().contains("gaac")||systemName.toLowerCase().contains("mobs")) {
        val arr = str.split(",")
        val hostname = getAPPTRANSHostName(arr(1))
        val dateStr = arr(2)
        val timeStr=arr(3)
        val dateTime=getTransTimeStamp(dateStr,timeStr)
        arr.update(1,hostname)
        arr.update(2, dateTime)
        arr.update(3, dateTime)
        formatString = arr.mkString(",")
      } else {
        val arr = str.split(delimiter)
        val dateStr = getITMTimeStamp(arr(1))
        arr.update(1, dateStr)
        formatString = arr.mkString(",")
      }
      formatString
    } catch {
      case e: Exception => println("InitUtil.dataFormat wrong data format")
        "NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR"
    }

  }

  /**
    * 根据ip获取OPM主机名信息，ip到主机的映射
    * @param str
    * @return
    */
  def getOPMDBHostName(str: String): String = {
    var hostname = "NONE_EXIST:ERROR"
    if (OPMDBHostsMap.containsKey(str)) {
      hostname = OPMDBHostsMap.get(str) + ":" + str
    }
    hostname.toUpperCase()
  }

  /**
    * 根据ip获取OPM主机名信息，ip到主机的映射
    * @param str
    * @return
    */
  def getAPPTRANSHostName(str: String): String = {
    var hostname = "NONE_EXIST:ERROR"
    if (APPTRANSHostsMap.containsKey(str)) {
      hostname = APPTRANSHostsMap.get(str) + ":" + str
    }
    hostname.toUpperCase()
  }

  /**
    * 格式化WAS系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getTransTimeStamp(orgDate: String,orgTime:String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val dateStr=orgDate.replaceAll("-","")
    val timeStr=orgTime.replaceAll(":","")
    val date = simpleDateFormat.parse(dateStr+timeStr)
    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
    val temp = simpleDateFormatStd.format(date)
    temp
  }

  /**
    * 格式化WAS系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getWASTimeStamp(str: String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = simpleDateFormat.parse(str)
    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
    val temp = simpleDateFormatStd.format(date)
    temp
  }
  /**
    * 格式化GMM系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getGMMTimeStamp(str: String): String = {
    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = simpleDateFormatStd.parse(str)
    val simpleDateFormatTemp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val dateTemp = simpleDateFormatTemp.format(date)
    val timeStampStr = dateTemp.split("-")
    val timeStamp = timeStampStr(0) + "," + timeStampStr(1) + "," + timeStampStr(2) + "," + timeStampStr(3) + "," + timeStampStr(4)
    timeStamp
  }
  /**
    * 格式化ITM系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getITMTimeStamp(str: String): String = {
//    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateStr = "20" + str.substring(1, 13)
//    val date = simpleDateFormat.parse(dateStr)
//    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
//    val temp = simpleDateFormatStd.format(date)
    dateStr
  }
  /**
    * 格式化离线Network系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getOfflineNetworkTimeStamp(str: String): String = {
    val dateStr = new Date((str + "000").toLong)
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = simpleDateFormat.format(dateStr)
    date
  }
  /**
    * 格式化实时Network系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getNetworkTimeStamp(str: String): String = {
    val dateStr = new Date((str + "000").toLong)
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = simpleDateFormat.format(dateStr)
    date
  }
  /**
    * 格式化实时OPMDB系统时间戳为yyyyMMddHHmmss形式
    * @param str
    * @return
    */
  def getOPMDBTimeStamp(dateStrTemp: String): String = {
    @transient
    val logger = Logger.getLogger(this.getClass)
    try {
      val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      val simpleDateFormatBOC = new SimpleDateFormat("yyyyMMddHHmmss")
      val date = simpleDateFormatORG.parse(dateStrTemp)
      val dateStr = simpleDateFormatBOC.format(date)
      dateStr
    } catch {
      case e: Exception =>
        try {
          val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
          val simpleDateFormatBOC = new SimpleDateFormat("yyyyMMddHHmmss")
          val date = simpleDateFormatORG.parse(dateStrTemp)
          val dateStr = simpleDateFormatBOC.format(date)
          dateStr
        } catch {
          case e: Exception =>
            logger.error("日期格式不支持")
            ""
        }

    }

  }


  /**
    * 获取所需字段
    *
    * @param line one row of a file
    * @return fields that stripped unused fields
    */
  def getNeededFields(line: String, list: util.ArrayList[Int], delimiter: String): String = {
    @transient
    val logger = Logger.getLogger(this.getClass)
    val allFields: Array[String] = line.split(delimiter)
    var sb = new StringBuffer()
    try {
      for (i <- 0 until list.size()) {
        sb.append(allFields(list.get(i)))
        if (i < list.size() - 1) {
          sb.append(",")
        }
      }
      //      time = convertTimestamp(allFields(list.get(0)))
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("allFields.size" + allFields.length + "------list.size" + list.size() + "------list" + list.toArray.mkString(",") + "++**********this line is not handled:" + line + "**********")
        sb = new StringBuffer()

    }
    sb.toString
  }

  def eachStripTimestamp(file: String): Array[String] = {
    val lines = file.split("\n")
    val formatArr = lines.map(stripTimestamp(_)).filter(!_.equals(""))
    formatArr
  }

  /**
    * 去除时间戳字段，方便计算
    * @param x
    * @return
    */
  def stripTimestamp(x: String): String = {
    if (x.contains(",")) {
      val cols: Array[String] = x.split(",")
      val arr = cols.tail
      arr.mkString(",")
    } else {
      //TODO
      ""
    }
  }

  /**
    * 加载傅里叶变换模型
    * @param rootPath
    * @return
    */
  def loadDFTModel(rootPath: String): ConcurrentHashMap[String, Array[Double]] = {
    @transient
    val logger = Logger.getLogger(this.getClass)
    //数据缓存队列
    val loadModelStartTime = System.currentTimeMillis()
    val map = new ConcurrentHashMap[String, Array[Double]]
    val pools = Executors.newFixedThreadPool(50)

    for (i <- 0 until list.size()) {
      //      println("*************************i:"+i+"*************************")
      val initBean = list.get(i)
      val runnable = new Runnable {
        override def run(): Unit = {
          val loadStartTime = System.currentTimeMillis()
          var arr = new Array[Double](288 * 7)
          try {
            val filePath: String = rootPath + initBean.getSystemName + "/" + initBean.getHostName + "/" + initBean.getTargetName + "/" + initBean.getHostName + ".txt"
            val path = new Path(filePath)
            val conf = new Configuration()
            val fs = FileSystem.newInstance(conf)

            val is = fs.open(path)

            arr = fillModel2(is)
            val loadEndTime = System.currentTimeMillis()
            logger.info("*************************com.wisdom.spark.etl.util.InitUtil.loadDFTModel加载" + initBean.getSystemName + "_" + initBean.getHostName + "_" + initBean.getTargetName + "模型耗时:" + (loadEndTime - loadStartTime) + "ms*************************")

            map.put(initBean.getSystemName + "_" + initBean.getHostName + "_" + initBean.getTargetName, arr)
          } catch {
            case e: Exception =>
              map.put(initBean.getSystemName + "_" + initBean.getHostName + "_" + initBean.getTargetName, arr)
              logger.error("*************************com.wisdom.spark.etl.util.InitUtil:" + initBean.getSystemName + "_" + initBean.getHostName + "_" + initBean.getTargetName + "傅里叶变换模型不存在*************************")
          }

        }
      }
      pools.execute(runnable)
    }
    //直到加载完成
    while (map.size() != list.size()) {
      logger.warn("*************************map.size:" + map.size() + "*************************list.size()" + list.size())
      Thread.sleep(1000 * 2)
    }
    pools.shutdown()

    val loadModelEndTime = System.currentTimeMillis()
    logger.warn("*************************加载傅里叶变换模型耗时:" + (loadModelEndTime - loadModelStartTime) + "ms*************************")
    map
  }

  /**
    *
    * @param file
    * @return
    */
  def fillModel(file: Array[String]): Array[Complex] = {
    val arr = new Array[Complex](288 * 7)
    for (i <- 0 until file.length) {
      val rows = file(i)
      val re = rows.split(",")(0).toDouble
      val im = rows.split(",")(1).toDouble
      val complex = new Complex(re, im)
      arr(i) = complex
    }
    arr
  }


  def fillModel2(is: InputStream): Array[Double] = {
    val arr = new Array[Double](288 * 7)
    val isr = new InputStreamReader(is)
    val br = new BufferedReader(isr)
    var i = 0
    var rows = br.readLine()
    while (rows != null) {
      arr(i) = rows.toDouble
      i = i + 1
      rows = br.readLine()
    }
    arr
  }


  /**
    * 每小时 1 到 12 个点
    *
    * 每一周 周日 1 周一 2 ...周六 7
    *
    * @param timeStamp 交行格式时间戳
    * @return 傅里叶变换索引值
    */
  def getFFTIndex(timeStamp: String): Int = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = simpleDateFormat.parse(timeStamp)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val minute = calendar.get(Calendar.MINUTE)
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    val day = calendar.get(Calendar.DAY_OF_WEEK)
    var minuteIndex = 0
    val hourIndex = hour * 12

    //    val startDateStr=ConfUtil.FourierModelStartTime+"000000"
    //    val startDateStr=""
    //    val startDate=simpleDateFormat.parse(startDateStr)
    //    val calendarFFT=Calendar.getInstance()
    //    calendarFFT.setTime(startDate)
    //    val dayFFT=calendarFFT.get(Calendar.DAY_OF_WEEK)
    //    //偏移量
    //    val offset= dayFFT - 1
    val dayIndex = (day - 1) * 288

    for (i <- 0 to 12) {
      if (minute >= 5 * i && minute < 5 * (i + 1)) {
        minuteIndex = i + 1
      }
    }
    val fftIndex = dayIndex + hourIndex + minuteIndex
    fftIndex
  }

  //  def idft_n(y: Array[Complex], n: Int): Complex = {
  //
  //    @transient
  //    val logger = Logger.getLogger(this.getClass)
  //    val N: Int = y.length
  //    var real: Double = 0
  //    var imag: Double = 0
  //    for (k <- 0 until N) {
  //      //        logger.error("*************************k:"+k+"*************************")
  //      if (y(k).re == 0 && y(k).im == 0) {
  //
  //      } else {
  //        val th: Double = 2 * Math.PI * k * n / N
  //        real += y(k).re * Math.cos(th) - y(k).im * Math.sin(th)
  //        imag += y(k).im * Math.cos(th) + y(k).re * Math.sin(th)
  //      }
  //
  //    }
  //    return new Complex(real / N, imag / N)
  //  }

  def isNumeric(str: String): Boolean = {
    val reg = "[-?\\d+[.\\d+]?,]+-?\\d+[.\\d+]?||-?\\d+[.\\d+]?"
    str.matches(reg)
  }

  def NaNToNumeric(str: String): String = {
    val numStr=str.toLowerCase().replaceAll(ETL_REGEX,"0")

    numStr
  }

  def minus(str1: String, str2: String): String = {
    val arr1 = str1.split(",")
    val arr2 = str2.split(",")

    val arr3 = arr1.zipAll(arr2, "0", "0")
    val arr4 = arr3.map { case (num1, num2) => num1.toDouble - num2.toDouble }
    arr4.mkString(",")
  }

  def minus(arr1: Array[String], arr2: Array[String]): Array[String] = {
    val temp = arr1.zip(arr2)
    val temp2 = temp.map { case (arr1, arr2) => minus(arr1, arr2) }
    temp2
//    temp2.mkString("\n")
  }

  def afterPeriod(str: String): Boolean = {
    try {
      val timestamp = str.split(",")(0).toLong
      if (timestamp > trainFileStartTime) {
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        println("afterPeriod Error")
        //        logger.error("afterPeriod Error")
        false
    }

  }

  /**
    * 获取傅里叶变换一周连续的数据
    * @param predictionFieldIndex
    * @param column_current
    * @return
    */
  def selectOneWeekData(predictionFieldIndex: Int, column_current: Array[String]): Array[Double] = {
    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
    val simpleDateFormatTemp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    @transient
    val logger = Logger.getLogger(this.getClass)

    val oneWeekData = new ArrayBuffer[Double]()
    val map = new util.HashMap[String, Array[Double]]

    var defaultArr = new Array[Double](288)

    var i = 0

    try{
      while (i < column_current.length) {
        val row = column_current(i)
        val cols = row.split(",")
        val dateStartPoint = cols(0)
        //选取配置的时间戳之后的数据
        if (FourierModelStartTime < dateStartPoint.toLong) {
          val dateStart = simpleDateFormatStd.parse(dateStartPoint)
          val dateStr = simpleDateFormatTemp.format(dateStart)
          val timestamp = dateStr.split("-")
          val firstPoint = timestamp(0) + timestamp(1) + timestamp(2) + timestamp(3) + timestamp(4) + timestamp(5)
          //每天的第一个点开始，范围00-05
          val firstPointMin = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "00" + "00"
          //每天的最后一个点结束，范围00-05
          val firstPointMax = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "04" + "59"
          if (isFistOrLastPoint(firstPointMin, firstPoint, firstPointMax)) {
            //如果大于，直接跳到最后，不再循环遍历
            if ((i + 287) < column_current.length) {
              val rowLast = column_current(i + 287)
              val colsLast = rowLast.split(",")
              val dateStartPointLast = colsLast(0)
              val dateStartLast = simpleDateFormatStd.parse(dateStartPointLast)
              val dateStrLast = simpleDateFormatTemp.format(dateStartLast)
              val timestampLast = dateStrLast.split("-")

              val lastPoint = timestampLast(0) + timestampLast(1) + timestampLast(2) + timestampLast(3) + timestampLast(4) + timestampLast(5)
              //每天的最后一个点的开始，范围55-00
              val lastPointMin = timestampLast(0) + timestampLast(1) + timestampLast(2) + "23" + "55" + "00"
              //每天的最后一个点的结束，范围55-00
              val lastPointMax = timestampLast(0) + timestampLast(1) + timestampLast(2) + "23" + "59" + "59"
              if (isFistOrLastPoint(lastPointMin, lastPoint, lastPointMax)) {
                val day = DateUtil.DayOfWeek(dateStartPoint)
                //数据是否存在于map中
                if (!map.containsKey("day" + day)) {
                  val arr = new Array[Double](288)
                  var j = i
                  var k = 0
                  while (j < column_current.length && j < i + 288) {
                    val currentData = column_current(j)
                    val actual = currentData.split(",")(predictionFieldIndex)
                    arr(k) = actual.toDouble
                    j += 1
                    k += 1
                  }
                  map.put("day" + day, arr)
                  i = j
                  defaultArr = arr
                } else {
                  i = i + 287
                }
              } else {
                i = i + 287
              }
            } else {
              i = column_current.length
            }
          } else {
            i = i + 1
          }
        }else{
          i=i+1
        }
      }
      for (i <- 1 until 8) {
        if (map.containsKey("day" + i)) {
          defaultArr = map.get("day" + i)
        }
        defaultArr.map(oneWeekData.append(_))

      }
    }catch {
      case e:Exception=>
        logger.error("init.selectOneWeekData error "+e.printStackTrace())

    }


    oneWeekData.toArray
  }

  def isFistOrLastPoint(firstPointMin: String, firstPoint: String, firstPointMax: String): Boolean = {
    try {
      val min = firstPointMin.toLong
      val value = firstPoint.toLong
      val max = firstPointMax.toLong
      if (value >= min && value <= max) {
        true
      } else {
        false
      }

    } catch {
      case e: Exception => println("com.wisdom.spark.etl.util.FFTUtil数据转换异常")
        false
    }

  }


}
