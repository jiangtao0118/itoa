package com.wisdom.spark.streaming.service

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import org.apache.log4j.Logger
import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.streaming.tools.ConnUtils

/**
  * Created by wisdom on 2017/5/15.
  */
object IndexService {

  //从数据库查询到的每一条数据保存到case class里面
  case class DataRecord(
                         tgtvar: String,
                         hostnode: String,
                         period: String,
                         model_typ: String,
                         flag: String,
                         recordTime: String,
                         reserveCol1: String,
                         reserveCol2: String
                       )

  val props = ItoaPropertyUtil.getProperties()
  val logger = Logger.getLogger(this.getClass)

  //获取数据库信息
  //获取mysql驱动
  val driver = props.getProperty("mysql.db.driver")
  //获取mysql的url
  val url = props.getProperty("mysql.db.url")
  // 获取mysql用户名
  val userName = props.getProperty("mysql.db.user")
  //获取mysql的密码
  val userPwd = props.getProperty("mysql.db.pwd")

  //将所有预测时间保存到数组
  val period = Array(5, 15, 30, 60)
  //将所有预测类型保存到数据
  val model = Array("PPN", "ST")
  //flag保存预测有效性，0代表无效，1代表有效
  val flag = Array(0, 1)
  var conn: Connection = null
  //将所有预测指标保存到数据
  val arr = Array("JVM_MEMORY_USED_KB",
    "AVAIL_REAL_MEM_PCT",
    "IDLE_CPU",
    "CPUUTILIZATION",
    "MEMORYUTILIZATION",
    "CLIENTNEWCONNECTIONS",
    "SERVERCURCONNECTIONS",
    "AVAIL_SWAP_SPACE_PCT",
    "ECUPIN_AVG_TRANS_TIME",
    "ECUPOUT_AVG_TRANS_TIME",
    "GAAC_AVG_TRANS_TIME",
    "MOBS_AVG_TRANS_TIME"
  )

  //实现从T_TABLE_HOST_INDEX表获取所有指标的主机名，并将所有数据插入到t_index_alarm_type表中
  def getHostnameByIndex: Unit = {
    conn = ConnUtils.getConn()
    var pst1: PreparedStatement = null
    for (i <- 0 until arr.length) {
      //创建查询所有指标对应的主机名
      var sql = "select hostName from T_TABLE_HOST_INDEX where indexName=" + "\"" + arr(i) + "\""
      pst1 = conn.prepareStatement(sql)
      //执行查询
      var res = pst1.executeQuery()
      while (res.next()) {
        var hostnode = res.getString(1)
        //调用dataInsert函数，将所有数据插入到表中
        dataInsert(arr(i), hostnode)
      }
    }
    ConnUtils.closeConn(conn)
  }


  //实现从T_TABLE_HOST_INDEX表获取所有指标的主机名，关联指标名，预测时间，模型类型，是否有效，创建时间，等信息插入到t_index_alarm_type
  def dataInsert(indexname: String, hostname: String): Unit = {

    var pst2: PreparedStatement = null
    //获取不同的预测时间
    for (i <- 0 until period.length) {
      //获取不同的预测模型
      for (j <- 0 until model.length) {
        //获取是否有效
        for (k <- 0 until flag.length) {
          //创建插入时间格式
          var df = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
          //创建插入语句
          var sql = "insert into t_index_alarm_type(tgtvar,hostnode,period,model_typ,flag,recordTime,reserveCol1,reserveCol2) values('" + indexname + "','" + hostname + "','" + period(i) + "','" + model(j) + "','" + flag(k) + "','" + (df.format(new Date().getTime)) + "'," + null + "," + null + ")"

          pst2 = conn.prepareStatement(sql)
          //将所有语句存储到Batch里面
          pst2.addBatch()
          //执行插入语句
        }
      }
    }
    pst2.executeBatch()
  }


  //查找对应指标所在主机在预测时间里的数据
  def queryData(): util.ArrayList[DataRecord] = {
    conn = ConnUtils.getConn()
    //新建List，将结果保存在List里面
    var list: util.ArrayList[DataRecord] = null
    var pst: PreparedStatement = null
    //创建查询语句
    val sql = "select * from t_index_alarm_type"
    pst = conn.prepareStatement(sql)
    //执行查询
    val res = pst.executeQuery()
    //将list赋值给包含DataRecord类型的数据
    list = new util.ArrayList[DataRecord]()
    while (res.next()) {
      val datarecord = DataRecord(res.getString(1), res.getString(2), res.getString(3), res.getString(4), res.getString(5), res.getString(6), res.getString(7), res.getString(8))
      list.add(datarecord)
    }
    ConnUtils.closeConn(conn)
    list
  }


  def main(args: Array[String]): Unit = {

    //数据插入
    getHostnameByIndex
    //数据查询并输出
    println(queryData())
  }
}


