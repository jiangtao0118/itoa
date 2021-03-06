package com.wisdom.spark.streaming.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.wisdom.spark.common.util.SparkContextUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/6/12.
  */
object Hivetest {

  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getInstance()
    //    新建HiveContext对象
    val hiveCtx = new HiveContext(sc)
    UpdateHiveInput(hiveCtx)

  }

  def UpdateHiveInput(hiveCtx: HiveContext) : Unit = {

    val time = new Date()
    val lastoccurtime = getlastmonthtime(time)
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val courencetime = simpleDateFormatORG.format(time)
//    val tempTable2 = hiveCtx.sql("select concat(substring(dtimestamp,12,4),case when substring(dtimestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
//      "substring(system_name,1,instr(system_name,':')-1) as system_name,idle_cpu from hist_itmnew.system where length(trim(idle_cpu)) > 0 and dtimestamp < '" + courencetime + "' and dtimestamp >  '" + lastoccurtime + "'")
//    //    注册临时表
//    val table1 = hiveContext.sql("select concat(substring(opm_min_collection_timestamp,12,4),case when substring(opm_min_collection_timestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
//    " opm_db_host_name,appls_in_db2 from hist_opm.opm_db where length(trim(appls_in_db2)) > 0 and opm_min_collection_timestamp < \'" + courencetime + "\' and opm_min_collection_timestamp >  \'" + lastoccurtime + "\' limit 10 ")
//    //val list = table.map(row => println(row))
//    val table2 = hiveContext.sql("select concat(substring(opm_min_collection_timestamp,12,4),case when substring(opm_min_collection_timestamp,16,1) < 5 then '0' else '5' end) as alarmTime," +
//      " opm_db_host_name,appls_in_db2 from hist_opm.opm_db where length(trim(appls_in_db2)) > 0 and opm_min_collection_timestamp < '" + courencetime + "' and opm_min_collection_timestamp >  '" + lastoccurtime + "' limit 10 ")
//    val len1 = table1.collect().toList
//    val len2 = table2.collect().toList
    val tempTableapp1 = hiveCtx.sql("select dtimestamp as time,avg_trans_time as value,metric_value as hostname from hist_apptrans.ecupin where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"' limit 10")
    val tempTableopm1 = hiveCtx.sql("select opm_min_collection_timestamp as time,appls_cur_cons as value,opm_db_host_name as hostname from hist_opm.opm_db where opm_min_collection_timestamp > '"+lastoccurtime +"' and opm_min_collection_timestamp < '"+courencetime+"' limit 10")
    val tempTableitm1 = hiveCtx.sql("select dtimestamp as time,idle_cpu as value,system_name as hostname from hist_itm.system where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"' limit 10")
    val tempTablenco1 = hiveCtx.sql("select from_unixtime(lastoccurrence) as time,mibvalue as value,node as hostname,alertgroup as indexname from ncoperf.reporter_status where from_unixtime(lastoccurrence) > '"+lastoccurtime +"' and from_unixtime(lastoccurrence) < '"+courencetime+"' limit 10")
    //len1.map(row => println(row))
    println("==================================分界线=========================================="+tempTableapp1.show(10))
    println("==================================分界线=========================================="+tempTableopm1.show(10))

    println("==================================分界线=========================================="+tempTableitm1.show(10))
    println("==================================分界线=========================================="+tempTablenco1.show(10))
    //len2.map(row => println(row))
   // println("==================================分界线=========================================="+len2.size)
  }

  private def getlastmonthtime(date:Date)  :String = {
    val simpleformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val cal = Calendar.getInstance()
    val time = cal.setTime(date)
    val lasttime= cal.add(Calendar.WEEK_OF_YEAR,-1)
    val finaltime = cal.getTime
    val st = simpleformat.format(finaltime)
    st
  }

}
