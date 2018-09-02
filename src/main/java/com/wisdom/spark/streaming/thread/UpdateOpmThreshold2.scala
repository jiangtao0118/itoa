package com.wisdom.spark.streaming.thread

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.streaming.bean.AlarmConfiguration
import com.wisdom.spark.streaming.dao.AlarmDao
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/6/9.
  */
object UpdateOpmThreshold2 {


  @transient
  //日志实例
  val logger = Logger.getLogger(this.getClass)
  //获取数据库连接
  //val conn = ConnPoolUtil2.getConn()
  //  新建AlarmDao对象
  val alarmDao = new AlarmDao
  //  配置文件
  //val props = ItoaPropertyUtil.getProperties()

  def main(args: Array[String]) {
    //公共方法：获取SparkContext
    val sc = SparkContextUtil.getInstance()
    //    新建HiveContext对象
    val hiveCtx = new HiveContext(sc)
    val props = ItoaPropertyUtil.getProperties()
    val conn = ConnPoolUtil2.getConn()
    //更新网络动态阈值
    updateOpmCurThreshold(hiveCtx,conn,props)
    //    更新指标规则配置关系表
    alarmDao.updateROmpCrel(conn)
    //释放连接
    ConnPoolUtil2.releaseCon(conn)
    //    停止程序
    sc.stop()
  }

  //更新网络动态阈值
  private def updateOpmCurThreshold(hiveCtx: HiveContext,conn:Connection,props: Properties): Unit = {
    //查询Hive，统计出节点、告警时刻、指标名和指标值
    val time = new Date()
    val lastoccurtime = UpdateOpmThreshold2.getlastmonthtime(time)
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val courencetime = simpleDateFormatORG.format(time)
    //将表中的所有主机名存到List里面
    //val list = queryhostname(conn)
    val list1 = props.getProperty("data.opm.cur.cons.hostname").split(",").toList
    //    sql:查询出目标时间区间内的所有数据，并做时间戳转换（unixtime转换成hh:m0/hh:mm5的格式）
    val tempTable1 = hiveCtx.sql("select concat(substring(opm_min_collection_timestamp,12,4),case when substring(opm_min_collection_timestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
      " opm_db_host_name,appls_cur_cons from hist_opm.opm_db where length(trim(appls_cur_cons)) > 0 and opm_min_collection_timestamp < '" + courencetime + "' and opm_min_collection_timestamp >  '" + lastoccurtime + "'")
    //    注册临时表
    tempTable1.registerTempTable("tempOpmcurperf")
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~~~hist_opm.opm_db注册临时表结束~~~~~~~~~~~~~~~~~~~~~·")
    //    sql:按照字段node,alarmTime,alertgroup,devvendor进行分组，将同一alarmTime的mibvalue进行列转行，用逗号分隔
    val threshold_table1 = hiveCtx.sql("select opm_db_host_name,alarmTime,concat_ws(',',collect_list(concat(appls_cur_cons))) as appls_cur_cons " +
      " from tempOpmcurperf  group by opm_db_host_name,alarmTime")

    //缓存RDD
    threshold_table1.persist()
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~RDD缓存结束~~~~~~~~~~~~~~~~~~~~~~~~")
    //    计算DataFrame每一行数据中mibvalue的平均值和方差
    val alarmRu1 =threshold_table1.map(row => row)

    val alarmRulesDF1 =alarmRu1.filter(row => list1.contains(row.getString(0))).map(row => {
      //统计字符串形式的数组，求出平均值和方差

      val mibvalues = row.getString(2).split(",").map(a => a.toDouble)
      //      预测值阈值：mibvalues平均值
      val mid_threshold = UpdateOpmThreshold2.ARRstdev(mibvalues).avg
      //      预测值下限阈值：mibvalues平均值-3倍方差
      var low_threshold = mid_threshold - 3 * (UpdateOpmThreshold2.ARRstdev(mibvalues).stdev)
      if (low_threshold < 0) low_threshold = 0
      //      预测值上限阈值：mibvalues平均值+3倍方差
      val high_threshold = mid_threshold + 3 * (UpdateOpmThreshold2.ARRstdev(mibvalues).stdev)
      //      主机名
      val hostname = row.getString(0)
      //      时刻00:05
      val alarmTime = row.getString(1)
      //      指标名
      val predIndexName = "APPLS_CUR_CONS"
      //      告警信息描述
      val confDesc = "网络指标" + predIndexName + "告警阈值配置"
      //      计算结果封装AlarmConfiguration
      val ac: AlarmConfiguration = new AlarmConfiguration()
      ac.setSysName("OPM_DB")
      ac.setHostName(hostname)
      ac.setIndexTyp("00")
      ac.setAlarmTime(alarmTime)
      ac.setPredIndexName(predIndexName)
      ac.setConfDesc(confDesc)
      ac.setConfMidValue(mid_threshold.toString)
      ac.setConfLowValue(low_threshold.toString)
      ac.setConfHighValue(high_threshold.toString)
      ac
    })


    val tempTable2 = hiveCtx.sql("select concat(substring(opm_min_collection_timestamp,12,4),case when substring(opm_min_collection_timestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
      " opm_db_host_name,appls_in_db2 from hist_opm.opm_db where length(trim(appls_in_db2)) > 0 and opm_min_collection_timestamp < '" + courencetime + "' and opm_min_collection_timestamp >  '" + lastoccurtime + "'")
    //    注册临时表
    tempTable2.registerTempTable("tempOpminperf")
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~~~hist_opm.opm_db注册临时表结束~~~~~~~~~~~~~~~~~~~~~·")
    //    sql:按照字段node,alarmTime,alertgroup,devvendor进行分组，将同一alarmTime的mibvalue进行列转行，用逗号分隔
    val threshold_table2 = hiveCtx.sql("select opm_db_host_name,alarmTime,concat_ws(',',collect_list(concat(appls_in_db2))) as appls_in_db2 " +
      " from tempOpminperf  group by opm_db_host_name,alarmTime")

    //缓存RDD
    threshold_table2.persist()
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~RDD缓存结束~~~~~~~~~~~~~~~~~~~~~~~~")
    val list2 = props.getProperty("data.opm.in.db2.hostname").split(",").toList
    //   将DataFrame类型数据转成RDD类型
    val alarmRu2 =threshold_table2.map(row => row)
    //过滤掉不存在List中的数据
    val alarmRulesDF2 =alarmRu2.filter(row => list2.contains(row.getString(0))).map(row => {

      //统计字符串形式的数组，求出平均值和方差

      val mibvalues = row.getString(2).split(",").map(a => a.toDouble)
      //      预测值阈值：mibvalues平均值
      val mid_threshold = UpdateOpmThreshold2.ARRstdev(mibvalues).avg
      //      预测值下限阈值：mibvalues平均值-3倍方差
      var low_threshold = mid_threshold - 3 * (UpdateOpmThreshold2.ARRstdev(mibvalues).stdev)
      if (low_threshold < 0) low_threshold = 0
      //      预测值上限阈值：mibvalues平均值+3倍方差
      val high_threshold = mid_threshold + 3 * (UpdateOpmThreshold2.ARRstdev(mibvalues).stdev)
      //      主机名
      val hostname = row.getString(0)
      //      时刻00:05
      val alarmTime = row.getString(1)
      //      指标名
      val predIndexName = "APPLS_IN_DB2"
      //      告警信息描述
      val confDesc = "网络指标" + predIndexName + "告警阈值配置"
      //      计算结果封装AlarmConfiguration
      val ac: AlarmConfiguration = new AlarmConfiguration()
      ac.setSysName("OPM_DB")
      ac.setHostName(hostname)
      ac.setIndexTyp("00")
      ac.setAlarmTime(alarmTime)
      ac.setPredIndexName(predIndexName)
      ac.setConfDesc(confDesc)
      ac.setConfMidValue(mid_threshold.toString)
      ac.setConfLowValue(low_threshold.toString)
      ac.setConfHighValue(high_threshold.toString)
      ac
    })

    //删除t_alarm_configuration中sysName="OPM_DB"的所有条目
    alarmDao.delAlarmConf("OPM_DB", conn)

    val alarmRulesDF = alarmRulesDF1.union(alarmRulesDF2)
    val partitions = props.getProperty("kafka.data.partitions").toInt
    val fist = alarmRulesDF.repartition(partitions).collect().toList
   //将结果保存到t_alarm_configuration表中
    alarmDao.saveAlarmConf(fist, conn)
  }


    //求取当前时间的上一个月时间点
    private def getlastmonthtime(date: Date): String =
    {
      val simpleformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      val cal = Calendar.getInstance()
      val time = cal.setTime(date)
      val lasttime= cal.add(Calendar.MONTH,-2)
      //val lasttime = cal.add(Calendar.DAY_OF_MONTH, -1)
      val finaltime = cal.getTime
      val st = simpleformat.format(finaltime)
      st
    }

  //查询数据库中所有的主机名，保存到List里面
  private def queryhostname(conn: Connection): util.ArrayList[String] = {
    var list: util.ArrayList[String] = new util.ArrayList[String]()
    var pst: PreparedStatement = null
    val sql = "select hostName from t_table_host_index group by hostName"
    pst = conn.prepareStatement(sql)
    val querylist = pst.executeQuery()
    while (querylist.next()) {
      val query = querylist.getString(1)
      list.add(query)
    }
    list
  }
    //求取Double数组的平均值和标准差
    def ARRstdev(arrDbl: Array[Double]): statistic = {
      //        平均值
      val avg = arrDbl.sum / arrDbl.length
      //        方差
      var sum = 0.0
      for (i <- arrDbl) {
        sum += math.pow(i - avg, 2)
      }
      val stdev = math.sqrt(sum / (arrDbl.length - 1))
      //        返回值statistic（均值，标准差）
      statistic(avg, stdev)
    }

    //  用于保存mibvalue均值方差的class
    case class statistic(avg: Double, stdev: Double)

  }


