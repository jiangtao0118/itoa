package com.wisdom.spark.etl.util

import java.text.SimpleDateFormat
import java.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by htgeng on 2017/1/18.
  */
object FFTUtil {
  def main(args: Array[String]) {
    val previous = "08,06,00,05"
    val current = "08,06,00,10"

    println(dataIsContinuous(previous, current))


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

      //      if (value >= min && value <= max) {
      //        if (DateUtil.isSunday(firstPoint)) {
      //          true
      //        } else {
      //          println("非周日")
      //          false
      //        }
      //      } else {
      ////        println("非第一个点")
      //        false
      //      }
    } catch {
      case e: Exception => println("com.wisdom.spark.etl.util.FFTUtil数据转换异常")
        false
    }

  }

  def selectOneWeekData(predictionFieldIndex: Int, column_current: Array[String]): Array[Double] = {
    val simpleDateFormatStd = new SimpleDateFormat("yyyyMMddHHmmss")
    val simpleDateFormatTemp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

    val oneWeekData = new ArrayBuffer[Double]()
    val map = new util.HashMap[String, Array[Double]]

    var defaultArr = new Array[Double](288)

    var i = 0

    while (i < column_current.length) {
      val row = column_current(i)
      val cols = row.split(",")
      val dateStartPoint = cols(0)

      val dateStart = simpleDateFormatStd.parse(dateStartPoint)
      val dateStr = simpleDateFormatTemp.format(dateStart)
      val timestamp = dateStr.split("-")
      val firstPoint = timestamp(0) + timestamp(1) + timestamp(2) + timestamp(3) + timestamp(4) + timestamp(5)
      val firstPointMin = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "00" + "00"
      val firstPointMax = timestamp(0) + timestamp(1) + timestamp(2) + "00" + "04" + "59"
      if (isFistOrLastPoint(firstPointMin, firstPoint, firstPointMax)) {
        if ((i + 287) < column_current.length) {
          val rowLast = column_current(i + 287)
          val colsLast = rowLast.split(",")
          val dateStartPointLast = colsLast(0)
          val dateStartLast = simpleDateFormatStd.parse(dateStartPointLast)
          val dateStrLast = simpleDateFormatTemp.format(dateStartLast)
          val timestampLast = dateStrLast.split("-")
          val lastPoint = timestampLast(0) + timestampLast(1) + timestampLast(2) + timestampLast(3) + timestampLast(4) + timestampLast(5)
          val lastPointMin = timestampLast(0) + timestampLast(1) + timestampLast(2) + "23" + "55" + "00"
          val lastPointMax = timestampLast(0) + timestampLast(1) + timestampLast(2) + "23" + "59" + "59"
          if (isFistOrLastPoint(lastPointMin, lastPoint, lastPointMax)) {
            val day = DateUtil.DayOfWeek(dateStartPoint)
            if(!map.containsKey("day"+day)){
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
            }else{
              i=i+287
            }


          } else {
            i = i + 1
          }
        } else {
          i = i + 287
        }
      } else {
        i = i + 1
      }

    }
    for (i <- 1 until 8) {
      if (map.containsKey("day" + i)) {
        defaultArr = map.get("day" + i)
      }
      defaultArr.map(oneWeekData.append(_))

    }
    oneWeekData.toArray
  }


  def dataIsContinuous(previousData: String, currentData: String): Boolean = {
    val previousCols = previousData.split(",")
    val currentCols = currentData.split(",")

    //    val previousDateStr="2016"+previousCols(0)+previousCols(1)+previousCols(2)+previousCols(3)
    //    val currentDateStr="2016"+currentCols(0)+currentCols(1)+currentCols(2)+currentCols(3)

    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

    val previousDate = simpleDateFormat.parse(previousCols(0))
    val currentDate = simpleDateFormat.parse(currentCols(0))

    if ((currentDate.getTime - previousDate.getTime) == 5 * 60 * 1000) {
      true
    } else {
      false
    }
  }

}
