package com.wisdom.spark.streaming.thread

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import com.wisdom.spark.common.util.{ConnPoolUtil2, ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.streaming.bean.AlarmConfiguration
import com.wisdom.spark.streaming.dao.AlarmDao
import com.wisdom.spark.streaming.tools.{Thread4SaveWINSTATSResult, ThreadPools}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/4/28.
  *
  * 动态阈值更新程序
  * 注：运行本程序需要将ItoaPropertyUtil中设置
  * var mlPath = "ml_properties.properties"
  * var etlPath = "etl_properties.properties"
  * var streamPath = "streaming_properties.properties"
  * 注意配置项中连接池最大连接数，推荐：2
  */
object UpdateThreshold {
  @transient
  //日志实例
  val logger = Logger.getLogger(this.getClass)
  //  新建AlarmDao对象
  val alarmDao = new AlarmDao

  def main(args: Array[String]) {
    //公共方法：获取SparkContext
    val sc = SparkContextUtil.getInstance()
    //    新建HiveContext对象
    val hiveCtx = new HiveContext(sc)
    // 获取属性配置文件
    val props = ItoaPropertyUtil.getProperties()
    // 建立数据库连接
    val conn = ConnPoolUtil2.getConn()
    //更新网络动态阈值
    updateNcoThreshold(hiveCtx,conn,props)
    //    更新指标规则配置关系表
    alarmDao.updateRNcoCrel(conn)
    //释放连接
    ConnPoolUtil2.releaseCon(conn)
    //    停止程序
    sc.stop()
  }

  //更新网络动态阈值
  private def updateNcoThreshold(hiveCtx: HiveContext,conn:Connection,props: Properties): Unit = {
    //查询Hive，统计出节点、告警时刻、指标名和指标值
    val time = new Date()
    // 获取当前时间前两个月的时间点
    val lastoccurtime = UpdateThreshold.getlastmonthtime(time)
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    // 获取当前时间点
    val courencetime = simpleDateFormatORG.format(time)
    // 查询t_table_host_index,将表中的所有主机名存到List里面
    val list = queryhostname(conn)
    //    sql:查询出目标时间区间内的所有数据
    val tempTable = hiveCtx.sql("select concat(substring(dtimestamp,12,4),case when substring(dtimestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
      " node,alertgroup,mibvalue from hist_ncoperf.reporter_status where length(trim(mibvalue)) > 0 and dtimestamp < '" + courencetime +"' and dtimestamp > '" + lastoccurtime +"'")
    //    注册临时表
    tempTable.registerTempTable("tempNcoperf")
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~~~ncoperf.reporter_status注册临时表结束~~~~~~~~~~~~~~~~~~~~~·")
    //    sql:按照字段node,alarmTime,alertgroup,进行分组，将同一alarmTime的mibvalue进行列转行，用逗号分隔
    val threshold_table = hiveCtx.sql("select node,alarmTime,alertgroup,concat_ws(',',collect_list(concat(mibvalue))) as mibvalue " +
      " from tempNcoperf  group by node,alarmTime,alertgroup")
    //缓存RDD
    threshold_table.persist()
    logger.warn("~~~~~~~~~~~~~~~~~~~~~~RDD缓存结束~~~~~~~~~~~~~~~~~~~~~~~~")
    //    将DataFrame类型转化成RDD
    val alarmRu1 =threshold_table.map(row => {
      row.getString(3).length > 0
      row })
    // 过滤掉不存在表t_table_host_index中的主机和指标数据
    val alarmRulesDF =alarmRu1.filter(row => {
      val data = DataRecord(row.getString(2),row.getString(0))
      list.contains(data)
    }).map(row => {
      //      指标名
      val predIndexName = row.getString(2)
      //统计字符串形式的数组，求出平均值和方差
      val mibvalues = row.getString(3).split(",").map(a => a.toDouble)
      //      预测值阈值：mibvalues平均值
      var mid_threshold = UpdateThreshold.ARRstdev(mibvalues).avg
      //      预测值下限阈值：mibvalues平均值-3倍标准差
      var low_threshold = mid_threshold - 3 * (UpdateThreshold.ARRstdev(mibvalues).stdev)
      //如果下限值小于0，那么置为0
      if (low_threshold < 0) low_threshold = 0
      //      预测值上限阈值：mibvalues平均值+3倍方差
      var high_threshold = mid_threshold + 3 * (UpdateThreshold.ARRstdev(mibvalues).stdev)
      // CpuUtilization 和 MemoryUtilization 两个指标如果上限值超过100，那么置为100
      if (predIndexName.equalsIgnoreCase("CpuUtilization") || predIndexName.equalsIgnoreCase("MemoryUtilization")) {
        if (high_threshold > 100) high_threshold =100
        if (mid_threshold > 100) mid_threshold =100
      }
      //      主机名
      val hostname = row.getString(0)
      //      时刻00:05
      val alarmTime = row.getString(1)
      //      告警信息描述
      val confDesc = "网络指标" + predIndexName + "告警阈值配置"
      //      计算结果封装AlarmConfiguration
      val ac: AlarmConfiguration = new AlarmConfiguration()
      ac.setSysName("reporter_status")
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
    //    删除t_alarm_configuration中sysName="reporter_status"的所有条目
    alarmDao.delAlarmConf("reporter_status", conn)
    logger.warn("**** 开始批量保存告警阈值... ===> ")
    //    获取重新分区数量
    val partitions = props.getProperty("kafka.data.partitions").toInt
    // 最大线程数量
    val maxThreadNums = props.getProperty("spark.driver.thread.max").toInt
    // 所有RDD转化为List
    val fist = alarmRulesDF.repartition(partitions).collect().toList
    // List的大小
    val len = fist.size
    // 多线程执行数据插入操作
    val high = len/maxThreadNums + 1
    for (i <- 0 until high){
      var li : List[AlarmConfiguration] = List()
      var flag = true
      for(j <- i*maxThreadNums until len if flag){
        if (j < (i+1)*maxThreadNums) {
          val alarmConfiguration = fist(j)
          li:+=alarmConfiguration
        }else{
          flag = false
        }
      }
      val conn1 = ConnPoolUtil2.getConn()
      //    保存阈值配置信息到t_alarm_configuration中
      ThreadPools.getPools().execute(new ThreadUpdateThreshold(li,conn1))
    }
    //alarmDao.saveAlarmConf(fist, conn)

  }

  // 查询t_table_host_index表，将表中的指标名和主机名存放到list中
  private  def queryhostname(conn:Connection) :util.ArrayList[DataRecord] ={
    var list :util.ArrayList[DataRecord] = new util.ArrayList[DataRecord]()
    var pst:PreparedStatement = null
    val sql = "select indexName,hostName from t_table_host_index group by indexName,hostName "
    pst = conn.prepareStatement(sql)
    val querylist = pst.executeQuery()
    while (querylist.next()) {
      val query = DataRecord(querylist.getString(1),querylist.getString(2))
      list.add(query)
    }
    list
  }

  // 获取当前时间前两个月的时间点
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

  // 求数组所有元素的平均值和标准差
  private def ARRstdev(arrDbl: Array[Double]): statistic = {
    //        平均值
    val avg = arrDbl.sum / arrDbl.length
    //        标准差
    var sum = 0.0
    for (i <- arrDbl) {
      sum += math.pow(i - avg, 2)
    }
    val stdev = math.sqrt(sum / (arrDbl.length - 1))
    //        返回值statistic（均值，标准差）
    statistic(avg, stdev)
  }

  private

  //  用于保存mibvalue均值方差的class
  case class statistic(avg: Double, stdev: Double)

  // 用于保存指标名和主机名的class
  case class  DataRecord(indexName: String,
                         hostName : String
                        )

}
