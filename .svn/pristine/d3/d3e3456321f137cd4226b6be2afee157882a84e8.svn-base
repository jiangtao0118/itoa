package com.wisdom.spark.etl.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.DecimalFormat
import java.util
import java.util.Properties

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.etl.bean.InitBean
import com.wisdom.spark.etl.dao.MysqlDao
import com.wisdom.spark.streaming.tools.ConnUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * Created by htgeng on 2017/6/28.
  */
object StatErrorCount {
  def main(args: Array[String]) {
    val props = ItoaPropertyUtil.getProperties()
    val ETL_SQL = props.getProperty("ETL_SQL".toUpperCase())
    val threshold = props.getProperty("alarm.result.tolerance").toInt

    val lists = getHostNameAndIndex(ETL_SQL)

    for (i <- 0 until lists.size()) {
      val bean = lists.get(i)
      val data = getOneDayData(bean, threshold)

      printf("%20s,%40s,%5s,%10s,%10s,%10s",bean.getHostName,bean.getTargetName,data._1,data._2,data._3,data._4)


//      println(s"bean.getHostName + "_" + bean.getTargetName + data + "   "")


    }

  }

  def getHostNameAndIndex(ETL_SQL: String): util.ArrayList[InitBean] = {
    val list = MysqlDao.selectBean(ETL_SQL)
    list
  }

  def getOneDayData(initBean: InitBean, threshold: Int): (Int, String, String, String) = {
    //批量执行SQL的条数
    //SQL：向MySQL中的t_pred_result中插入预测结果
    val ab = new ArrayBuffer[Double]
    var pstSelect: PreparedStatement = null
    var resultSet: ResultSet = null
    val sqlSelect = "select currentActualValue,nextPredMaxValue from t_pred_result where hostName= ? and predIndexName= ? order by predId desc limit 288"
    var conn: Connection = null
    var count = 0
    try {
      conn = ConnPoolUtil2.getConn()
      pstSelect = conn.prepareStatement(sqlSelect)
      pstSelect.setString(1, initBean.getHostName) //系统名称中文名
      pstSelect.setString(2, initBean.getTargetName) //主机名

      resultSet = pstSelect.executeQuery()
      while (resultSet.next()) {
        val actualValue = resultSet.getString(1).toDouble
        val maxValue = resultSet.getString(2).toDouble
        if (maxValue != 0.0) {
          val exceed = (actualValue - maxValue) / maxValue
          val percent = exceed * 100

          if (percent > threshold) {
            ab.append(percent)
            count = count + 1
          }
        }
      }

    } catch {
      case e: Exception => println("SQL Exception!!!数据库操作异常!!:addBatchPredResult()" + e.getMessage + e.printStackTrace())
    } finally {
      if (conn != null) {
        //释放数据库连接
        ConnPoolUtil2.releaseCon(conn)
      }
      ConnUtils.closeStatement(pstSelect, null, resultSet)
    }
    val array = ab.toArray
    Sorting.quickSort(array)
    val df = new DecimalFormat("0.00")
    var first = "0.00"
    var second = "0.00"
    var third = "0.00"
    val array1 = array.reverse
    if (array.length >= 3) {
      first = df.format(array1.head)
      val array2 = array1.tail
      second = df.format(array2.head)
      val array3 = array2.tail
      third = df.format(array3.head)
      //      first+"   "+count+"   "+second+"   "+third

    } else if (array.length == 2) {
      first = df.format(array1.head)
      val array2 = array1.tail
      second = df.format(array2.head)
      //      ""
    } else if (array.length == 1) {
    } else {
    }
    (count, first, second, third)
  }

}
