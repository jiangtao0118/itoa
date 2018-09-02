package com.wisdom.spark.etl.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by wisdom on 2016/12/13.
  */
object DateUtil {
  def main(args: Array[String]) {
    val date=new Date(System.currentTimeMillis())
    val sdf=new SimpleDateFormat("yyyy-MM-dd-HH")
    val dateStr=sdf.format(date)
    val dateArr=dateStr.split("-")
    val delStart=dateArr(0)+dateArr(1)+dateArr(2)+"15"
    val delEnd=dateArr(0)+dateArr(1)+dateArr(2)+"17"
    val delDate=dateArr(0)+dateArr(1)+dateArr(2)+dateArr(3)


    val cal = Calendar.getInstance()
    cal.setTime(date)
    val day=cal.get(Calendar.DAY_OF_WEEK)
    println("day:"+day)
    if(day==2){
      if(delDate.toInt>=delStart.toInt && delDate.toInt<delEnd.toInt){
        println("hello")
      }
    }




//    println(date.toString)
//    println(System.currentTimeMillis())
  }

  /**
    * 将交通银行的时间戳转换为数据库时间戳，精确到秒
    *
    * @param dateStrTemp 交通银行格式时间戳
    * @return 数据库时间戳，getTime/1000精确到秒
    */
  def getTimeStamp(dateStrTemp: String): Long = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateStr = "20" + dateStrTemp.substring(1, 13)
    val date = simpleDateFormat.parse(dateStr)
    date.getTime / 1000
  }

  def getBOCTimeStamp(dateStrTemp: String): String = {
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val simpleDateFormatBOC = new SimpleDateFormat("yyMMddHHmmssSSS")
    val date = simpleDateFormatORG.parse(dateStrTemp)
    val dateStr = "1" + simpleDateFormatBOC.format(date)
    dateStr
  }

  def getBOCTimeStamp2(dateStrTemp: String): String = {
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val simpleDateFormatBOC = new SimpleDateFormat("yyMMddHHmmssSSS")
    val date = simpleDateFormatORG.parse(dateStrTemp)
    val dateStr = "1" + simpleDateFormatBOC.format(date)
    dateStr
  }

  def DayOfWeek(dateStrTemp: String): Int = {
    try {
      val simpleDateFormatORG = new SimpleDateFormat("yyyyMMddHHmmss")
      val date = simpleDateFormatORG.parse(dateStrTemp)
      val cal = Calendar.getInstance()
      cal.setTime(date)
      val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)
      dayOfWeek
//      if (dayOfWeek == day) {
//        (true,
//      } else {
//        (false
//      }
    } catch {
      case e: Exception => println("isSunday error")
        6
    }
  }

  def getITMTimeStamp(str: String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateStr = "20" + str.substring(1, 13)
    val date = simpleDateFormat.parse(dateStr)
    val simpleDateFormatStd = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val temp = simpleDateFormatStd.format(date)
    temp
  }
}
