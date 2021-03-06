package com.wisdom.spark.ml.model.dynamicModel

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.wisdom.spark.common.util.{ConnPoolUtil2, SparkContextUtil}
import com.wisdom.spark.etl.bean.DBDataBean
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.tools.{ConnUtils, DateFormatUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.util.StatCounter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by htgeng on 2017/6/29.
  */
object StatFromHive {
  def main(args: Array[String]) {
    val conn = ConnPoolUtil2.getConn()
    val hostName = "ASGAAC01"
    val indexName = "IDLE_CPU"
    val modelMap = new mutable.HashMap[String,String]

    println("modelMap Length" + modelMap.keys.toList.length)
    val key = modelMap.keys.head
    println("modelMap key" +modelMap.get(key).get)

    if (conn != null) {
      //释放数据库连接
      ConnPoolUtil2.releaseCon(conn)
    }

  }


  def modelDataFormat(dataArray: Array[DBDataBean],window:Int): mutable.HashMap[String, ArrayBuffer[Double]] = {
    val weekMap = new mutable.HashMap[String, ArrayBuffer[Double]]()
    val winStatMap = new mutable.HashMap[String, ArrayBuffer[Double]]()

    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    for (data <- dataArray) {
      var sb: ArrayBuffer[Double] = null
      val date = sdf.parse(data.getDateTime)
      val key = getKey(date)
      val value = data.getValue.toDouble
      if (weekMap.contains(key)) {
        sb = weekMap.get(key).get
      } else {
        sb = new ArrayBuffer[Double]()
      }
      sb.append(value)
      weekMap.put(key, sb)
    }

    val keys=weekMap.keys
    for(key<-keys){

    }

    val dateStr="2018-08-08-"

    for (key <- keys) {
      val dayOfWeek=key.split("_")(0)
      val time=key.split("_")(1).replace(":","-")
      val dateTime=dateStr+time+"-00"
      val date=sdf.parse(dateTime)

      var sbStat: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      var sb1: ArrayBuffer[Double] = null
      if (weekMap.contains(key)) {
        sb1 = weekMap.get(key).get
        //        println("key:"+key+"!!!!!!!!sb1:"+sbStat.toString())
        sbStat.++=(sb1)
        val longTime = date.getTime
        //        val window = 3
        for (i <- 1 until window) {
          var sb_minus: ArrayBuffer[Double] = null
          var sb_plus: ArrayBuffer[Double] = null
          val window_minus = longTime - i * 300 * 1000
          val window_plus = longTime + i * 300 * 1000

          val date_minus = new Date(window_minus)

          val date_plus = new Date(window_plus)

          val key_minus = getKey(dayOfWeek,date_minus)
          val key_plus = getKey(dayOfWeek,date_plus)

          if (weekMap.contains(key_minus)) {
            sb_minus = weekMap.get(key_minus).get
            //            println("!!!!!!!!sb1:"+sb.toString())
            sbStat.++=(sb_minus)
            //            println("key_minus:"+key_minus+"!!!!!!!!sb2:"+sbStat.toString())
          }
          if (weekMap.contains(key_plus)) {
            sb_plus = weekMap.get(key_plus).get
            //            println("!!!!!!!!sb2:"+sb.toString())
            sbStat.++=(sb_plus)
            //            println("key_plus:"+key_plus+"!!!!!!!!sb3:"+sbStat.toString())
          }
        }

        //        println("--------sbAll:"+sbStat.toString())
        winStatMap.put(key, sbStat)
      }
    }
    winStatMap
  }

  def getModel(hiveCtx:HiveContext,indexInfoMap:mutable.HashMap[String,String],hostname: String, indexName: String,count:Int,window:Int): mutable.HashMap[String, (Double, Double)] = {
    val avg_std_map = new mutable.HashMap[String, (Double, Double)]

    val data = getDataFromHive(hiveCtx,indexInfoMap,hostname, indexName,count)

    if(data!=null){
      val formatDataMap = modelDataFormat(data,window)
      val keys = formatDataMap.keys
      for (key <- keys) {
        val ab = formatDataMap.get(key).get
        val sc = new StatCounter()
        sc.merge(ab)
        val stdev = sc.sampleStdev
        val mean = sc.mean
        val abFilter = ab.filter(x => Math.abs(x - mean) <= stdev)
        val scFilter = new StatCounter()
        scFilter.merge(abFilter)

        val meanFilter = scFilter.mean
        val stdevFilter = scFilter.sampleStdev

        avg_std_map.put(key, (meanFilter, stdevFilter))

      }
    }
    avg_std_map

  }


  private def getKey(date: Date): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) % 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var hourStr = hour.toString
    if (hour < 10) {
      hourStr = "0" + hourStr
    }
    val minute = cal.get(Calendar.MINUTE) / 5 * 5 + 1
    var minuteStr = minute.toString
    if (minute < 10) {
      minuteStr = "0" + minuteStr
    }

    dayOfWeek + "_" + hourStr + ":" + minuteStr
  }

  private def getKey(week:String,date: Date): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var hourStr = hour.toString
    if (hour < 10) {
      hourStr = "0" + hourStr
    }
    val minute = cal.get(Calendar.MINUTE) / 5 * 5 + 1
    var minuteStr = minute.toString
    if (minute < 10) {
      minuteStr = "0" + minuteStr
    }

    week + "_" + hourStr + ":" + minuteStr

  }


  def getFromDB(conn: Connection, hostname: String, indexName: String,count:Int): Array[DBDataBean] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val ab = new ArrayBuffer[DBDataBean]()
    val sqlQuery = "select currentDataTime,currentActualValue from t_pred_result where hostName = ? and predIndexname = ? order by predId desc limit "+count
    var pstQuery: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      pstQuery = conn.prepareStatement(sqlQuery)
      pstQuery.setString(1, hostname) //实测值
      pstQuery.setString(2, indexName) //数据当中的时间戳（原为计算出的时刻，有偏差需修改）
      resultSet = pstQuery.executeQuery()

      if (resultSet != null) {
        while (resultSet.next()) {
          val dbBean = new DBDataBean
          val dateTimeTemp = resultSet.getString(1)
          val value = resultSet.getString(2)
          val dateTime = new Date(dateTimeTemp.toLong * 1000)
          val dateTimeStr = sdf.format(dateTime)
          dbBean.setDateTime(dateTimeStr)
          dbBean.setValue(value)

          ab.append(dbBean)
        }
      }
    } catch {
      case e: Exception => println("SQL Exception!!!数据库操作异常!!:addBatchPredResult()" + e.getMessage + e.printStackTrace())
    } finally {
      ConnUtils.closeStatement(pstQuery, null, resultSet)
    }

    ab.toArray
  }

  /**
    * 1根据指标， 获取映射表，包括哪个字段是时间，哪个字段是实际值等共6个
    * 2根据获取到的信息，查询hive表
    * 3获得结果，格式化时间戳字段
    * @param hostname
    * @param indexName
    * @param count
    * @return
    */
  def getDataFromHive(hiveCtx:HiveContext,indexInfoMap:mutable.HashMap[String,String],hostname: String, indexName: String,count:Int): Array[DBDataBean] = {
//    val hiveCtx=new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    var ab:Array[DBDataBean] = null
    var sqlTemp=""
    if(indexInfoMap.contains(indexName)){
      sqlTemp=indexInfoMap.get(indexName).get
      sqlTemp=sqlTemp.replace("HOSTNAME",hostname)
      sqlTemp=sqlTemp.replace("INDEXNAME",indexName)
      sqlTemp=sqlTemp.replace("COUNT",count.toString)

      val result=hiveCtx.sql(sqlTemp)
      println("!!!"+""+sqlTemp)

      val arr=result.map { row =>
        val datetimeTemp = row.get(0).toString
        println("!!!!!!!!!!!!!!!!!!!!!"+datetimeTemp+"!!!!!!!!!!!!!!!!!!!!!")
        val date=DateFormatUtil.dateFormat(indexName,datetimeTemp)
        val value=row.get(1).toString
        val dbBean = new DBDataBean
        val dateTime = new Date(date.toLong * 1000)
        val dateTimeStr = sdf.format(dateTime)
        dbBean.setDateTime(dateTimeStr)
        dbBean.setValue(value)
        dbBean
      }

      ab=arr.collect().filter(_.getDateTime!="")
    }

    ab
  }

}
