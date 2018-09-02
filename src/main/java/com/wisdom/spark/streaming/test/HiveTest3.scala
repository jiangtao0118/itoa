package com.wisdom.spark.streaming.test

import java.sql.{Connection, DriverManager}

import com.wisdom.spark.common.bean.PredResult
import com.wisdom.spark.common.util.ConnPoolUtil2
import com.wisdom.spark.streaming.bean.AlarmResult
import com.wisdom.spark.streaming.dao.AlarmDao
import com.wisdom.spark.streaming.service.AlarmService
import org.apache.log4j.Logger

/**
  * Created by wisdom on 2017/6/30.
  */
object HiveTest3 {

  val logger = Logger.getLogger(this.getClass)
  val alarmDao = new AlarmDao

  def main(args: Array[String]): Unit = {
//    val driver = "com.mysql.jdbc.Driver"
//    val url = "jdbc:mysql://localhost:3306/wsd"
//    //val userName = "root"
//    //val userPwd = "admin"
//    var conn :Connection=null
//    Class.forName(driver)
//    conn = DriverManager.getConnection(url,"", "")
    var conn =ConnPoolUtil2.getConn()

    val ar = new AlarmService
    var pred =new PredResult
    var listAR = List[AlarmResult]()
    pred.setPredId(2028)
    pred.setSysName("SYSTEM")
    pred.setHostName("ASGAAC01")
    pred.setPredIndexName("IDLE_CPU")
    pred.setCurrentTime("1498464036")
    pred.setIndexTyp("00")
    pred.setPredPeriod("00")
    pred.setDataAcceptTime("1497770006")
    pred.setCurrentDataTime("1497769960")
    pred.setCurrentActualValue("64")
    pred.setNextPredValue("1")
    pred.setNextPredMinValue("2")
    pred.setNextPredMaxValue("14")
    val arTriple:AlarmResult=ar.ruleFunc9(pred,conn)
    //ar.ruleFunc9(pred,conn)
    listAR = listAR.::(arTriple)
    alarmDao.saveAlarmResultList(listAR, conn)
    ConnPoolUtil2.releaseCon(conn)
  }

}
