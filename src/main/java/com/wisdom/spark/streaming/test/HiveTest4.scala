package com.wisdom.spark.streaming.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.wisdom.spark.common.util.SparkContextUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wisdom on 2017/7/3.
  */
object HiveTest4 {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getInstance()
    //    新建HiveContext对象
    val hiveCtx = new HiveContext(sc)
    queryhive(hiveCtx)
  }

  def queryhive(hiveCtx:HiveContext):Unit = {
    val time = new Date()
    val lastoccurtime= getlastmonthtime(time)
    val simpleDateFormatORG = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val courencetime = simpleDateFormatORG.format(time)
    val tempTableapp1 = hiveCtx.sql("select dtimestamp as time,avg_trans_time as value,metric_value as hostname from hist_apptrans.ecupin where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableapp1.registerTempTable("tempappecupin")
    val tempTableapp2 = hiveCtx.sql("select dtimestamp as time,avg_trans_time as value,metric_value as hostname from hist_apptrans.ecupout where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableapp2.registerTempTable("tempappecupout")
    val tempTableapp3 = hiveCtx.sql("select dtimestamp as time,avg_trans_time as value,metric_value as hostname from hist_apptrans.gaac where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableapp3.registerTempTable("tempappgaac")
    val tempTableapp4 = hiveCtx.sql("select dtimestamp as time,avg_trans_time as value,metric_value as hostname from hist_apptrans.mobs where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableapp4.registerTempTable("tempappmobs")
    val tableapptruns = tempTableapp1.unionAll(tempTableapp2).unionAll(tempTableapp3).unionAll(tempTableapp4)


    val tempTableopm1 = hiveCtx.sql("select opm_min_collection_timestamp as time,appls_cur_cons as value,opm_db_host_name as hostname from hist_opm.opm_db where opm_min_collection_timestamp > '"+lastoccurtime +"' and opm_min_collection_timestamp < '"+courencetime+"'")
    //tempTableopm1.registerTempTable("tempopmcur")
    val tempTableopm2 = hiveCtx.sql("select opm_min_collection_timestamp as time,appls_in_db2 as value,opm_db_host_name as hostname from hist_opm.opm_db where opm_min_collection_timestamp > '"+lastoccurtime +"' and opm_min_collection_timestamp < '"+courencetime+"'")
    //tempTableopm2.registerTempTable("tempopmin")
    val tableopm = tempTableopm1.unionAll(tempTableopm2)

    val tempTableitm1 = hiveCtx.sql("select dtimestamp as time,idle_cpu as value,system_name as hostname from hist_itm.system where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableitm1.registerTempTable("tempitmidle")
    val tempTableitm2 = hiveCtx.sql("select dtimestamp as time,avail_real_mem_pct as value,system_name as hostname from hist_itm.unix_memory where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableitm2.registerTempTable("tempitmreal")
    val tempTableitm3 = hiveCtx.sql("select dtimestamp as time,avail_swap_space_pct as value,system_name as hostname from hist_itm.unix_memory where dtimestamp > '"+lastoccurtime +"' and dtimestamp < '"+courencetime+"'")
    //tempTableitm3.registerTempTable("tempitmspace")
    val  tableitm = tempTableitm1.unionAll(tempTableitm2).unionAll(tempTableitm3)

    val tempTablenco1 = hiveCtx.sql("select from_unixtime(lastoccurrence) as time,mibvalue as value,node as hostname,alertgroup as indexname from ncoperf.reporter_status where from_unixtime(lastoccurrence) > '"+lastoccurtime +"' and from_unixtime(lastoccurrence) < '"+courencetime+"'")
    //tempTablenco1.registerTempTable("tempitmspace")


  }

  private def getlastmonthtime(date: Date): String =
  {
    val simpleformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val cal = Calendar.getInstance()
    val time = cal.setTime(date)
    val lasttime= cal.add(Calendar.WEEK_OF_MONTH,-1)
    //val lasttime = cal.add(Calendar.DAY_OF_MONTH, -1)
    val finaltime = cal.getTime
    val st = simpleformat.format(finaltime)
    st
  }
}
