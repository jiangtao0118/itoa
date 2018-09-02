package com.wisdom.spark.streaming.tools

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger

/**
  * Created by htgeng on 2017/6/30.
  */
object DateFormatUtil extends Serializable{
  val sdfOrg = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val sdfDst = new SimpleDateFormat("yyyyMMddHHmmss")
  def dateFormat(indexName:String,dateStr: String): String = {
    var formatString = ""
    try {
      if ("IDLE_CPU,AVAIL_REAL_MEM_PCT,AVAIL_SWAP_SPACE_PCT".contains(indexName)) {
//        formatString=getITMTimeStamp(dateStr)
      } else if("CPUUTILIZATION,MEMORYUTILIZATION,IFUTILIZATION,CLIENTCURCONNECTIONS,CLIENTNEWCONNECTIONS,SERVERCURCONNECTIONS,CURCONNECTIONS".contains(indexName)){
        formatString=getOfflineNetworkTimeStamp(dateStr)
      }else if ("ECUPIN_AVG_TRANS_TIME,ECUPOUT_AVG_TRANS_TIME,GAAC_AVG_TRANS_TIME,MOBS_AVG_TRANS_TIME".contains(indexName)) {
        formatString=getTransTimeStamp(dateStr)
      }
    } catch {
      case e: Exception =>
        //        logger.error("wrong data format")
        println("wrong data format")
    }
    formatString

  }

  def dateFormat(dateStr: String): String = {
    var dateDst = ""
    try {
      val dateOrg=sdfOrg.parse(dateStr)
      dateDst=sdfDst.format(dateOrg)
    } catch {
      case e: Exception =>
        println("wrong data format")
    }
    dateDst
  }

  def getTransTimeStamp(orgDate: String): String = {
    val dateOrg = sdfOrg.parse(orgDate)
    val dateDst = sdfDst.format(dateOrg)
    dateDst
  }

//  def getITMTimeStamp(str: String): String = {
//    val dateOrg = sdfOrg.parse(orgDate)
//    val dateDst = sdfDst.format(dateOrg)
//    dateDst
//  }

  def getOfflineNetworkTimeStamp(str: String): String = {
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
}
