package com.wisdom.spark.streaming.thread

import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors}

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil}
import com.wisdom.spark.streaming.dao.AlarmDao
import com.wisdom.spark.streaming.bean.AlarmConfiguration

/**
  * Created by wisdom on 2017/6/21.
  */



class ThreadUpdateThreshold(alarmConfList: List[AlarmConfiguration],conn:Connection) extends Runnable {
   override def run(): Unit = {
     val alarmDao = new AlarmDao
     alarmDao.saveAlarmConf(alarmConfList, conn)
     ConnPoolUtil2.releaseCon(conn)
   }
}




