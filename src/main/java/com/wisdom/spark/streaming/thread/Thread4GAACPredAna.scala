package com.wisdom.spark.streaming.thread

import java.util.Properties

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.streaming.tools.JsonUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * GAAC性能分析
  * Created by zhengz on 2017/2/14.
  * 步骤：
  * 1.创建streaming-Context，采集周期1分钟
  * 2.Topic-APPTRANS消息接收DSm-APPTRANS
  * 3.Topic-OPM消息接收DSm-OPM
  * 4.Topic-ITM消息接收DSm-ITM
  * 5.针对ITM的DSm作ReduceByWindow操作，滑块设置1分钟；窗口设置5分钟，得到新 DSm
  * 6.连接MySQL获取主机和IP对应关系，存放Map[IP->Node]
  * 7.将多个DSM进行union，并作foreachRDD处理
  * 8.过滤rdd数据，分割为不同文件的rdd
  * 9.获取mysql中hive中表字段信息作为schema
  * 10.将rdd转换为dataframe
  * 11.将dataframe注册到表
  * 12.表与表进行关联，进行条件筛选、数据筛选
  *
  *
  */
object Thread4GAACPredAna extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)


  def main(args: Array[String]) {
    val sparkContext = SparkContextUtil.getInstance()
    val hiveContext = new HiveContext(sparkContext)
    val props = ItoaPropertyUtil.getProperties()

    /*获取主机IP配置信息*/
    val hostCfgDF = getHostConfig(hiveContext, props, "t_host_conf_info")

    val brokers = props.getProperty("kafka.common.brokers")

    val offset = props.getProperty("kafka.param.offset.value")



    hostCfgDF.registerTempTable("temp_host_conf_info")


    /*广播变量*/
    //    val initial = new InitUtil(props, null)
    val initialBroadCast = sparkContext.broadcast(props)


    /*获取相关HIVE表字段信息，生成schema*/
    val gaacCols = getColumns(hiveContext, "apptrans.gaac")
    val opmDbCols = getColumns(hiveContext, "opm.opm_db")
    val unixMemoryCols = getColumns(hiveContext, "itm.unix_memory")
    val systemCols = getColumns(hiveContext, "itm.system")
    val threadPoolsCols = getColumns(hiveContext, "itm.thread_pools")
    val dbConnectionPoolsCols = getColumns(hiveContext, "itm.db_connection_pools")
    val applicationServerCols = getColumns(hiveContext, "itm.application_server")
    val queueDataCols = getColumns(hiveContext, "itm.queue_data")



    val partitions = props.getProperty("kafka.data.partitions").toInt


    val sInterval = props.getProperty("gaac.kafka.stream.interval").toInt
    val itmTopic = props.getProperty("gaac.kafka.topic.itm").split(",").toSet
    val opmTopic = props.getProperty("gaac.kafka.topic.opm").split(",").toSet
    val apptransTopic = props.getProperty("gaac.kafka.topic.apptrans").split(",").toSet
    val kafkaParams = Map[String, String](props.getProperty("kafka.param.brokers.key") -> brokers, props.getProperty("kafka.param.offset.key") -> offset)

    val ssc = new StreamingContext(sparkContext, Seconds(sInterval))
    val itmDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, itmTopic).map(_._2).window(Seconds(sInterval * 10), Seconds(sInterval)).transform(_.distinct())
    val opmDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, opmTopic).map(_._2).transform(_.distinct())
    val apptransDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, apptransTopic).map(_._2).transform(_.distinct())

    val allDSM = apptransDSM.union(itmDSM).union(opmDSM).transform(_.repartition(partitions)).map(JsonUtil.parseJSON(_))





    allDSM.foreachRDD(rdd => {
      //获取广播变量
      val rddProps = initialBroadCast.value //需修改
      val itmBody = rddProps.getProperty("data.itm.value")

      /*将rdd根据文件名进行分离*/
      val gaacRDD = rdd.filter(checkContains(_, "gaac", rddProps))
      val opmDbRDD = rdd.filter(checkContains(_, "opmdb", rddProps))
      val unixMemoryRDD = rdd.filter(checkContains(_, "unixmemeory", rddProps))
      val systemRDD = rdd.filter(checkContains(_, "system", rddProps))
      val threadPoolsRDD = rdd.filter(checkContains(_, "threadpools", rddProps))
      val dbConnectionPoolsRDD = rdd.filter(checkContains(_, "dbconnectionpools", rddProps))
      val applicationServerRDD = rdd.filter(checkContains(_, "applicationserver", rddProps))
      val queueDataRDD = rdd.filter(checkContains(_, "queuedata", rddProps))
      /*将RDD映射到dataframe*/
      val gaacDF = hiveContext.createDataFrame(gaacRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), gaacCols)
      val opmDbDF = hiveContext.createDataFrame(opmDbRDD.map(m => Row.fromSeq(m(itmBody).toString.split("\\^").toSeq)), opmDbCols)
      val unixMemoryDF = hiveContext.createDataFrame(unixMemoryRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), unixMemoryCols)
      val systemDF = hiveContext.createDataFrame(systemRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), systemCols)
      val threadPoolsDF = hiveContext.createDataFrame(threadPoolsRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), threadPoolsCols)
      val dbConnectionPoolsDF = hiveContext.createDataFrame(dbConnectionPoolsRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), dbConnectionPoolsCols)
      val applicationServerDF = hiveContext.createDataFrame(applicationServerRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), applicationServerCols)
      val queueDataDF = hiveContext.createDataFrame(queueDataRDD.map(m => Row.fromSeq(m(itmBody).toString.split(",").toSeq)), queueDataCols)
      /*打印rdd和df*/
      logger.warn("##### rdd.take ===>" + rdd.take(5).mkString(","))
      logger.warn("##### gaacDF ===>")
      gaacDF.show(5)
      logger.warn("##### opmDbDF ===>")
      opmDbDF.show(5)

      /*将dataframe注册到临时表*/
      gaacDF.registerTempTable("temp_apptrans_gaac")
      opmDbDF.registerTempTable("temp_opm_opm_db")
      unixMemoryDF.registerTempTable("temp_itm_unix_memory")
      systemDF.registerTempTable("temp_itm_system")
      threadPoolsDF.registerTempTable("temp_itm_thread_pools")
      dbConnectionPoolsDF.registerTempTable("temp_itm_db_connection_pools")
      applicationServerDF.registerTempTable("temp_itm_application_server")
      queueDataDF.registerTempTable("temp_itm_queue_data")


      /*临时表关联取出服务器性能信息*/
      val gaacCfgDF = hiveContext.sql("SELECT DISTINCT t1.metric_name ,t1.metric_value ,t1.d_date ,t1.d_time ,trim(regexp_replace(concat(t1.d_date,substring(trim(t1.d_time),1,4),(case when substring(trim(t1.d_time),5,1) < '5' then '0' else '5' end)),'-|:','')) as d_timestamp0 ,trim(regexp_replace(concat(t1.d_date,t1.d_time),'-|:','')) as d_timestamp1 ,t1.trans_volume ,t1.trans_rate ,t1.avg_trans_time ,t2.cfghostname ,t2.cfgserverip ,t2.cfgservertype FROM  temp_apptrans_gaac t1 LEFT JOIN (  SELECT DISTINCT   hostName AS cfgHostName,   serverIP AS cfgServerIP,   (    CASE    WHEN isDB2 IN ('YES', 'Y') THEN     'isDB2'    WHEN isWAS IN ('YES', 'Y') THEN     'isWAS'    WHEN isMQ IN ('YES', 'Y') THEN     'isMQ'    ELSE     'OTHER'    END   ) AS cfgServerType  FROM   temp_host_conf_info ) t2 ON t1.metric_value = t2.cfgServerIP WHERE  t1.metric_name = '服务器IP'")
      gaacCfgDF.registerTempTable("temp_gaac_host_type")
      /*临时表关联取操作系统内存信息和cpu信息 unixmemory system*/
      val itmSysMemDF = hiveContext.sql("SELECT s1.dwritetime_format0, s1.dwritetime_format1, s1.tmzdiff, s1.dwritetime, s1.system_name, s1.dtimestamp, s1.total_virtual_storage_mb, s1.used_virtual_storage_mb, s1.avail_virtual_storage_mb, s1.virtual_storage_pct_used, s1.virtual_storage_pct_avail, s1.total_swap_space_mb, s1.used_swap_space_mb, s1.avail_swap_space_mb, s1.used_swap_space_pct, s1.avail_swap_space_pct, s1.total_real_mem_mb, s1.used_real_mem_mb, s1.avail_real_mem_mb, s1.used_real_mem_pct, s1.avail_real_mem_pct, s1.page_faults AS um_page_faults, s1.page_reclaims AS um_page_reclaims, s1.page_ins AS um_page_ins, s1.page_outs AS um_page_outs, s1.page_in_reqs, s1.page_out_reqs, s1.page_in_kb_s, s1.page_out_kb_s, s1.page_in_1min, s1.page_in_5min, s1.page_in_15min, s1.page_in_60min, s1.page_out_1min, s1.page_out_5min, s1.page_out_15min, s1.page_out_60min, s1.page_scan, s1.page_scan_kb, s1.page_scan_1min, s1.page_scan_5min, s1.page_scan_15min, s1.page_scan_60min, s1.arc_size_mb, s1.net_memory_used, s1.net_memory_avail, s1.net_memory_used_pct, s1.net_memory_avail_pct, s1.non_comp_memory, s1.comp_memory, s1.decay_rate, s1.repaging_rate, s1.pages_read_per_sec, s1.pages_written_per_sec, s1.paging_space_free_pct, s1.paging_space_used_pct, s1.paging_space_read_per_sec, s1.paging_space_write_per_sec, s1.comp_mem_pct, s1.non_comp_mem_pct, s1.filesys_avail_mem_pct, s1.memav_whsc, s1.rmava_whsc, s1.rmusd_whsc, s1.memus_whsc, s1.non_comp_mem_mb, s1.comp_mem_mb, s1.filesys_avail_mem_mb, s1.process_mem_pct, s1.prmep_whsc, s1.system_mem_pct, s1.symep_whsc, s1.file_repl_mem_pct, s1.frmep_whsc, s1.file_repl_min_mem_pct, s1.frmip_whsc, s1.file_repl_max_mem_pct, s1.frmap_whsc, s2.type, s2.version, s2.total_real_memory, s2.total_virtual_memory, s2.up_time, s2.users_session_number, s2.system_procs_number, s2.net_address, s2.user_cpu, s2.system_cpu, s2.idle_cpu, s2.wait_io, s2.processes_in_run_queue, s2.processes_waiting, s2.page_faults AS sys_page_faults, s2.page_reclaims AS sys_page_reclaims, s2.pages_paged_in, s2.pages_paged_out, s2.page_ins AS sys_page_ins, s2.page_outs AS sys_page_outs, s2.free_memory, s2.active_virtual_memory, s2.cpu_context_switches, s2.system_calls, s2.forks_executed, s2.execs_executed, s2.block_reads, s2.block_writes, s2.logical_block_reads, s2.logical_block_writes, s2.nonblock_reads, s2.nonblock_writes, s2.receive_interrupts, s2.transmit_interrupts, s2.modem_interrupts, s2.active_internet_connections, s2.active_sockets, s2.load_average_1_min, s2.load_average_5_min, s2.load_average_15_min, s2.dummy_memory_free, s2.memory_used, s2.page_scan_rate, s2.virtual_memory_percent_used, s2.vmfreeprc, s2.cpu_busy, s2.system_read, s2.system_write, s2.system_threads, s2.processes_runnable, s2.processes_running, s2.processes_sleeping, s2.processes_idle, s2.processes_zombie, s2.processes_stopped, s2.threads_in_run_queue, s2.threads_waiting, s2.boot_time, s2.pending_io_waits, s2.start_io, s2.device_interrupts, s2.uptime, s2.swap_space_free, s2.page_ins_rate, s2.page_out_rate, s2.page_scanning, s2.avg_pageins_1, s2.avg_pageins_5, s2.avg_pageins_15, s2.avg_pageins_60, s2.avg_pageout_1, s2.avg_pageout_5, s2.avg_pageout_15, s2.avg_pageout_60, s2.avg_pagescan_1, s2.avg_pagescan_5, s2.avg_pagescan_15, s2.avg_pagescan_60, s2.avg_processes_runqueue_60, s2.ipv6_address, s2.zone_id, s2.zone_name, s2.system_software_version, s2.number_of_cpus, s2.physical_consumption, s2.stolen_idle_cycles_pct, s2.stolen_busy_cycles_pct, s2.time_spent_in_hypervisor_pct, s2.bread_whsc, s2.bwrit_whsc, s2.cpubu_whsc, s2.unixi_whsc, s2.phrea_whsc, s2.phwri_whsc, s2.pc_whsc, s2.unixs_whsc, s2.unixu_whsc, s2.vmuse_whsc, s2.noc_whsc FROM ( SELECT concat( '20', substring(t1.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(t1.dwritetime, 2, 9), ( CASE WHEN substring(t1.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, t1.* FROM temp_itm_unix_memory t1 ) s1 LEFT JOIN ( SELECT concat( '20', substring(t2.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(t2.dwritetime, 2, 9), ( CASE WHEN substring(t2.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, t2.* FROM temp_itm_system t2 ) s2 ON s1.tmzdiff = s2.tmzdiff AND s1.dwritetime_format0 = s2.dwritetime_format0 AND s1.system_name = s2.system_name")
      itmSysMemDF.registerTempTable("temp_itm_memory_system")
      /*临时表关联获取gaac和itm的system表、memory表的关系基础表*/
      val gaacCfgItmBaseDF = hiveContext.sql("SELECT t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.d_timestamp0, t1.d_timestamp1, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t1.cfghostname, t1.cfgserverip, t1.cfgservertype, t2.dwritetime_format0, t2.dwritetime_format1, t2.tmzdiff, t2.dwritetime, t2.system_name, t2.dtimestamp, t2.total_virtual_storage_mb, t2.used_virtual_storage_mb, t2.avail_virtual_storage_mb, t2.virtual_storage_pct_used, t2.virtual_storage_pct_avail, t2.total_swap_space_mb, t2.used_swap_space_mb, t2.avail_swap_space_mb, t2.used_swap_space_pct, t2.avail_swap_space_pct, t2.total_real_mem_mb, t2.used_real_mem_mb, t2.avail_real_mem_mb, t2.used_real_mem_pct, t2.avail_real_mem_pct, t2.um_page_faults, t2.um_page_reclaims, t2.um_page_ins, t2.um_page_outs, t2.page_in_reqs, t2.page_out_reqs, t2.page_in_kb_s, t2.page_out_kb_s, t2.page_in_1min, t2.page_in_5min, t2.page_in_15min, t2.page_in_60min, t2.page_out_1min, t2.page_out_5min, t2.page_out_15min, t2.page_out_60min, t2.page_scan, t2.page_scan_kb, t2.page_scan_1min, t2.page_scan_5min, t2.page_scan_15min, t2.page_scan_60min, t2.arc_size_mb, t2.net_memory_used, t2.net_memory_avail, t2.net_memory_used_pct, t2.net_memory_avail_pct, t2.non_comp_memory, t2.comp_memory, t2.decay_rate, t2.repaging_rate, t2.pages_read_per_sec, t2.pages_written_per_sec, t2.paging_space_free_pct, t2.paging_space_used_pct, t2.paging_space_read_per_sec, t2.paging_space_write_per_sec, t2.comp_mem_pct, t2.non_comp_mem_pct, t2.filesys_avail_mem_pct, t2.memav_whsc, t2.rmava_whsc, t2.rmusd_whsc, t2.memus_whsc, t2.non_comp_mem_mb, t2.comp_mem_mb, t2.filesys_avail_mem_mb, t2.process_mem_pct, t2.prmep_whsc, t2.system_mem_pct, t2.symep_whsc, t2.file_repl_mem_pct, t2.frmep_whsc, t2.file_repl_min_mem_pct, t2.frmip_whsc, t2.file_repl_max_mem_pct, t2.frmap_whsc, t2.type, t2.version, t2.total_real_memory, t2.total_virtual_memory, t2.up_time, t2.users_session_number, t2.system_procs_number, t2.net_address, t2.user_cpu, t2.system_cpu, t2.idle_cpu, t2.wait_io, t2.processes_in_run_queue, t2.processes_waiting, t2.sys_page_faults, t2.sys_page_reclaims, t2.pages_paged_in, t2.pages_paged_out, t2.sys_page_ins, t2.sys_page_outs, t2.free_memory, t2.active_virtual_memory, t2.cpu_context_switches, t2.system_calls, t2.forks_executed, t2.execs_executed, t2.block_reads, t2.block_writes, t2.logical_block_reads, t2.logical_block_writes, t2.nonblock_reads, t2.nonblock_writes, t2.receive_interrupts, t2.transmit_interrupts, t2.modem_interrupts, t2.active_internet_connections, t2.active_sockets, t2.load_average_1_min, t2.load_average_5_min, t2.load_average_15_min, t2.dummy_memory_free, t2.memory_used, t2.page_scan_rate, t2.virtual_memory_percent_used, t2.vmfreeprc, t2.cpu_busy, t2.system_read, t2.system_write, t2.system_threads, t2.processes_runnable, t2.processes_running, t2.processes_sleeping, t2.processes_idle, t2.processes_zombie, t2.processes_stopped, t2.threads_in_run_queue, t2.threads_waiting, t2.boot_time, t2.pending_io_waits, t2.start_io, t2.device_interrupts, t2.uptime, t2.swap_space_free, t2.page_ins_rate, t2.page_out_rate, t2.page_scanning, t2.avg_pageins_1, t2.avg_pageins_5, t2.avg_pageins_15, t2.avg_pageins_60, t2.avg_pageout_1, t2.avg_pageout_5, t2.avg_pageout_15, t2.avg_pageout_60, t2.avg_pagescan_1, t2.avg_pagescan_5, t2.avg_pagescan_15, t2.avg_pagescan_60, t2.avg_processes_runqueue_60, t2.ipv6_address, t2.zone_id, t2.zone_name, t2.system_software_version, t2.number_of_cpus, t2.physical_consumption, t2.stolen_idle_cycles_pct, t2.stolen_busy_cycles_pct, t2.time_spent_in_hypervisor_pct, t2.bread_whsc, t2.bwrit_whsc, t2.cpubu_whsc, t2.unixi_whsc, t2.phrea_whsc, t2.phwri_whsc, t2.pc_whsc, t2.unixs_whsc, t2.unixu_whsc, t2.vmuse_whsc, t2.noc_whsc FROM temp_gaac_host_type t1 LEFT JOIN temp_itm_memory_system t2 ON locate( t1.cfghostname, t2.system_name ) > 0 AND t1.d_timestamp0 = t2.dwritetime_format1")
      gaacCfgItmBaseDF.registerTempTable("temp_gaac_cfg_itm_base")


      /*根据服务器类型选择性关联opm和itm表形成宽表 isDB2 || isWAS || isMQ || OTHER */
      /*cfgServerType = 'isDB2' apptrans.gaac itm.unix_memory itm.system opm.opm_db */
      val gaacIsDB2 = hiveContext.sql("SELECT t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.d_timestamp0, t1.d_timestamp1, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t1.cfghostname, t1.cfgserverip, t1.cfgservertype, t1.dwritetime_format0, t1.dwritetime_format1, t1.tmzdiff, t1.dwritetime, t1.system_name, t1.dtimestamp, t1.total_virtual_storage_mb, t1.used_virtual_storage_mb, t1.avail_virtual_storage_mb, t1.virtual_storage_pct_used, t1.virtual_storage_pct_avail, t1.total_swap_space_mb, t1.used_swap_space_mb, t1.avail_swap_space_mb, t1.used_swap_space_pct, t1.avail_swap_space_pct, t1.total_real_mem_mb, t1.used_real_mem_mb, t1.avail_real_mem_mb, t1.used_real_mem_pct, t1.avail_real_mem_pct, t1.um_page_faults, t1.um_page_reclaims, t1.um_page_ins, t1.um_page_outs, t1.page_in_reqs, t1.page_out_reqs, t1.page_in_kb_s, t1.page_out_kb_s, t1.page_in_1min, t1.page_in_5min, t1.page_in_15min, t1.page_in_60min, t1.page_out_1min, t1.page_out_5min, t1.page_out_15min, t1.page_out_60min, t1.page_scan, t1.page_scan_kb, t1.page_scan_1min, t1.page_scan_5min, t1.page_scan_15min, t1.page_scan_60min, t1.arc_size_mb, t1.net_memory_used, t1.net_memory_avail, t1.net_memory_used_pct, t1.net_memory_avail_pct, t1.non_comp_memory, t1.comp_memory, t1.decay_rate, t1.repaging_rate, t1.pages_read_per_sec, t1.pages_written_per_sec, t1.paging_space_free_pct, t1.paging_space_used_pct, t1.paging_space_read_per_sec, t1.paging_space_write_per_sec, t1.comp_mem_pct, t1.non_comp_mem_pct, t1.filesys_avail_mem_pct, t1.memav_whsc, t1.rmava_whsc, t1.rmusd_whsc, t1.memus_whsc, t1.non_comp_mem_mb, t1.comp_mem_mb, t1.filesys_avail_mem_mb, t1.process_mem_pct, t1.prmep_whsc, t1.system_mem_pct, t1.symep_whsc, t1.file_repl_mem_pct, t1.frmep_whsc, t1.file_repl_min_mem_pct, t1.frmip_whsc, t1.file_repl_max_mem_pct, t1.frmap_whsc, t1.type, t1.version, t1.total_real_memory, t1.total_virtual_memory, t1.up_time, t1.users_session_number, t1.system_procs_number, t1.net_address, t1.user_cpu, t1.system_cpu, t1.idle_cpu, t1.wait_io, t1.processes_in_run_queue, t1.processes_waiting, t1.sys_page_faults, t1.sys_page_reclaims, t1.pages_paged_in, t1.pages_paged_out, t1.sys_page_ins, t1.sys_page_outs, t1.free_memory, t1.active_virtual_memory, t1.cpu_context_switches, t1.system_calls, t1.forks_executed, t1.execs_executed, t1.block_reads, t1.block_writes, t1.logical_block_reads, t1.logical_block_writes, t1.nonblock_reads, t1.nonblock_writes, t1.receive_interrupts, t1.transmit_interrupts, t1.modem_interrupts, t1.active_internet_connections, t1.active_sockets, t1.load_average_1_min, t1.load_average_5_min, t1.load_average_15_min, t1.dummy_memory_free, t1.memory_used, t1.page_scan_rate, t1.virtual_memory_percent_used, t1.vmfreeprc, t1.cpu_busy, t1.system_read, t1.system_write, t1.system_threads, t1.processes_runnable, t1.processes_running, t1.processes_sleeping, t1.processes_idle, t1.processes_zombie, t1.processes_stopped, t1.threads_in_run_queue, t1.threads_waiting, t1.boot_time, t1.pending_io_waits, t1.start_io, t1.device_interrupts, t1.uptime, t1.swap_space_free, t1.page_ins_rate, t1.page_out_rate, t1.page_scanning, t1.avg_pageins_1, t1.avg_pageins_5, t1.avg_pageins_15, t1.avg_pageins_60, t1.avg_pageout_1, t1.avg_pageout_5, t1.avg_pageout_15, t1.avg_pageout_60, t1.avg_pagescan_1, t1.avg_pagescan_5, t1.avg_pagescan_15, t1.avg_pagescan_60, t1.avg_processes_runqueue_60, t1.ipv6_address, t1.zone_id, t1.zone_name, t1.system_software_version, t1.number_of_cpus, t1.physical_consumption, t1.stolen_idle_cycles_pct, t1.stolen_busy_cycles_pct, t1.time_spent_in_hypervisor_pct, t1.bread_whsc, t1.bwrit_whsc, t1.cpubu_whsc, t1.unixi_whsc, t1.phrea_whsc, t1.phwri_whsc, t1.pc_whsc, t1.unixs_whsc, t1.unixu_whsc, t1.vmuse_whsc, t1.noc_whsc, t2.opmtime_format0, t2.opmtime_format1, t2.opm_db_host_name, t2.opm_db_name, t2.opm_min_collection_timestamp, t2.db_status, t2.appls_cur_cons, t2.appls_in_db2, t2.total_log_used, t2.total_log_available, t2.lock_list_in_use, t2.lock_escals, t2.lock_timeouts, t2.locks_waiting, t2.deadlocks, t2.opm_transactions, t2.lock_wait_time, t2.opm_sort_time_per_sort, t2.opm_sorts_per_transaction, t2.sort_overflows, t2.rows_read, t2.opm_logical_reads, t2.pool_async_index_reads, t2.opm_bp_hitratio, t2.opm_log_buffer_hitratio, t2.opm_log_read_time_per_log_read, t2.opm_log_write_time_per_log_write, t2.num_log_read_io, t2.num_log_write_io, t2.log_disk_wait_time, t2.log_write_time, t2.opm_pool_read_time_per_read, t2.opm_pool_write_time_per_write FROM temp_gaac_cfg_itm_base t1 LEFT JOIN ( SELECT r0.opmtime_format0, concat( substring(r0.opmtime_format0, 1, 11), ( CASE WHEN substring(r0.opmtime_format0, 12, 1) < '5' THEN '0' ELSE '5' END )) AS opmtime_format1, r0.* FROM ( SELECT substring( regexp_replace ( s0.OPM_MIN_COLLECTION_TIMESTAMP, '-|:|\\.|T|Z', '' ), 1, 12 ) AS opmtime_format0, s0.* FROM opm.opm_db s0 ) r0 ) t2 ON t1.d_timestamp1 = t2.opmtime_format0 AND t1.cfgserverip = t2.opm_db_host_name WHERE t1.cfgServerType = 'isDB2'")
      /*模型预测 gaacIsDB2.map()*/
      val rdd1 = gaacIsDB2.map(("isdb2", _))


      /*cfgServerType = 'isWAS' apptrans.gaac itm.unix_memory itm.system itm.thread_pools itm.db_connection_pools itm.application_server */
      val gaacIsWAS = hiveContext.sql("SELECT t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.d_timestamp0, t1.d_timestamp1, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t1.cfghostname, t1.cfgserverip, t1.cfgservertype, t1.dwritetime_format0, t1.dwritetime_format1, t1.tmzdiff, t1.dwritetime, t1.system_name, t1.dtimestamp, t1.total_virtual_storage_mb, t1.used_virtual_storage_mb, t1.avail_virtual_storage_mb, t1.virtual_storage_pct_used, t1.virtual_storage_pct_avail, t1.total_swap_space_mb, t1.used_swap_space_mb, t1.avail_swap_space_mb, t1.used_swap_space_pct, t1.avail_swap_space_pct, t1.total_real_mem_mb, t1.used_real_mem_mb, t1.avail_real_mem_mb, t1.used_real_mem_pct, t1.avail_real_mem_pct, t1.um_page_faults, t1.um_page_reclaims, t1.um_page_ins, t1.um_page_outs, t1.page_in_reqs, t1.page_out_reqs, t1.page_in_kb_s, t1.page_out_kb_s, t1.page_in_1min, t1.page_in_5min, t1.page_in_15min, t1.page_in_60min, t1.page_out_1min, t1.page_out_5min, t1.page_out_15min, t1.page_out_60min, t1.page_scan, t1.page_scan_kb, t1.page_scan_1min, t1.page_scan_5min, t1.page_scan_15min, t1.page_scan_60min, t1.arc_size_mb, t1.net_memory_used, t1.net_memory_avail, t1.net_memory_used_pct, t1.net_memory_avail_pct, t1.non_comp_memory, t1.comp_memory, t1.decay_rate, t1.repaging_rate, t1.pages_read_per_sec, t1.pages_written_per_sec, t1.paging_space_free_pct, t1.paging_space_used_pct, t1.paging_space_read_per_sec, t1.paging_space_write_per_sec, t1.comp_mem_pct, t1.non_comp_mem_pct, t1.filesys_avail_mem_pct, t1.memav_whsc, t1.rmava_whsc, t1.rmusd_whsc, t1.memus_whsc, t1.non_comp_mem_mb, t1.comp_mem_mb, t1.filesys_avail_mem_mb, t1.process_mem_pct, t1.prmep_whsc, t1.system_mem_pct, t1.symep_whsc, t1.file_repl_mem_pct, t1.frmep_whsc, t1.file_repl_min_mem_pct, t1.frmip_whsc, t1.file_repl_max_mem_pct, t1.frmap_whsc, t1.type, t1.version, t1.total_real_memory, t1.total_virtual_memory, t1.up_time, t1.users_session_number, t1.system_procs_number, t1.net_address, t1.user_cpu, t1.system_cpu, t1.idle_cpu, t1.wait_io, t1.processes_in_run_queue, t1.processes_waiting, t1.sys_page_faults, t1.sys_page_reclaims, t1.pages_paged_in, t1.pages_paged_out, t1.sys_page_ins, t1.sys_page_outs, t1.free_memory, t1.active_virtual_memory, t1.cpu_context_switches, t1.system_calls, t1.forks_executed, t1.execs_executed, t1.block_reads, t1.block_writes, t1.logical_block_reads, t1.logical_block_writes, t1.nonblock_reads, t1.nonblock_writes, t1.receive_interrupts, t1.transmit_interrupts, t1.modem_interrupts, t1.active_internet_connections, t1.active_sockets, t1.load_average_1_min, t1.load_average_5_min, t1.load_average_15_min, t1.dummy_memory_free, t1.memory_used, t1.page_scan_rate, t1.virtual_memory_percent_used, t1.vmfreeprc, t1.cpu_busy, t1.system_read, t1.system_write, t1.system_threads, t1.processes_runnable, t1.processes_running, t1.processes_sleeping, t1.processes_idle, t1.processes_zombie, t1.processes_stopped, t1.threads_in_run_queue, t1.threads_waiting, t1.boot_time, t1.pending_io_waits, t1.start_io, t1.device_interrupts, t1.uptime, t1.swap_space_free, t1.page_ins_rate, t1.page_out_rate, t1.page_scanning, t1.avg_pageins_1, t1.avg_pageins_5, t1.avg_pageins_15, t1.avg_pageins_60, t1.avg_pageout_1, t1.avg_pageout_5, t1.avg_pageout_15, t1.avg_pageout_60, t1.avg_pagescan_1, t1.avg_pagescan_5, t1.avg_pagescan_15, t1.avg_pagescan_60, t1.avg_processes_runqueue_60, t1.ipv6_address, t1.zone_id, t1.zone_name, t1.system_software_version, t1.number_of_cpus, t1.physical_consumption, t1.stolen_idle_cycles_pct, t1.stolen_busy_cycles_pct, t1.time_spent_in_hypervisor_pct, t1.bread_whsc, t1.bwrit_whsc, t1.cpubu_whsc, t1.unixi_whsc, t1.phrea_whsc, t1.phwri_whsc, t1.pc_whsc, t1.unixs_whsc, t1.unixu_whsc, t1.vmuse_whsc, t1.noc_whsc, t2.thp_dwritetime_format0, t2.thp_dwritetime_format1, t2.dwritetime AS thp_dwritetime, t2.origin_node, t2.server_name, t2.node_name, t2.sample_date_and_time, t2.interval_time, t2.thread_pool_name, t2.set_instrumentation_level_type, t2.instrumentation_level, t2.summary_of_thread_pools, t2.threads_created, t2.thread_creation_rate, t2.threads_destroyed, t2.thread_destruction_rate, t2.average_active_threads, t2.average_pool_size, t2.percent_of_time_pool_at_max, t2.average_free_threads, t2.maximum_pool_size, t2.app_id AS thp_app_id, t2.percent_used_good, t2.percent_used_fair, t2.percent_used_bad, t3.dcp_dwritetime_format0, t3.dcp_dwritetime_format1, t3.dwritetime AS dcp_dwritetime, t3.origin_node AS dcp_origin_node, t3.server_name AS dcp_server_name, t3.node_name AS dcp_node_name, t3.sample_date_and_time AS dcp_sample_date_and_time, t3.interval_time AS dcp_interval_time, t3.instrumentation_level AS dcp_instrumentation_level, t3.datasource_name, t3.connections_created, t3.connections_destroyed, t3.connections_allocated, t3.threads_timed_out, t3.average_pool_size AS dcp_average_pool_size, t3.average_waiting_threads, t3.average_wait_time, t3.average_usage_time, t3.percent_used, t3.percent_of_time_pool_at_max AS dcp_percent_of_time_pool_at_max, t3.connection_creation_rate, t3.connection_destruction_rate, t3.connection_allocation_rate, t3.thread_timeout_rate, t3.summary_of_all_db_connections, t3.set_instrumentation_level_type AS dcp_set_instrumentation_level_type, t3.prep_statement_cache_discards, t3.rte_pscd, t3.return_count, t3.return_rate, t3.maximum_pool_size AS dcp_maximum_pool_size, t3.datasource_label, t3.connection_used, t3.total_usage_time, t3.total_wait_time, t3.connection_granted, t3.app_id AS dcp_app_id, t3.percent_used_good AS dcp_percent_used_good, t3.percent_used_fair AS dcp_percent_used_fair, t3.percent_used_bad AS dcp_percent_used_bad, t3.pool_size, t3.average_free_pool_size, t3.connectionhandle, t3.jdbc_time, t3.wait_time_count, t3.usage_time_count, t3.jdbc_time_count, t4.aps_dwritetime_format0, t4.aps_dwritetime_format1, t4.dwritetime AS aps_dwritetime, t4.origin_node AS aps_origin_node, t4.server_name AS aps_server_name, t4.node_name AS aps_node_name, t4.sample_date_and_time AS aps_sample_date_and_time, t4.interval_time AS aps_interval_time, t4.STATUS, t4.version as aps_version, t4.start_date_and_time, t4.process_id, t4.jvm_memory_free, t4.jvm_memory_total, t4.jvm_memory_used, t4.cpu_used, t4.cpu_used_percent, t4.instrumentation_level as aps_instrumentation_level, t4.server_type, t4.server_instance_name, t4.request_data_monitoring_level, t4.request_data_sampling_rate, t4.resource_data_monitoring, t4.garbage_collection_monitoring, t4.system_paging_rate, t4.jvm_memory_free_kb, t4.jvm_memory_total_kb, t4.jvm_memory_used_kb, t4.platform_cpu_used, t4.asid, t4.summary, t4.ve_host_PORT, t4.probe_id, t4.server_mode, t4.server_subnode_name, t4.hung_threads_blocked, t4.hung_threads_waiting, t4.hung_threads_timed_waiting, t4.hung_threads_total, t4.was_node_name, t4.was_cell_name FROM temp_gaac_cfg_itm_base t1 LEFT JOIN ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS thp_dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS thp_dwritetime_format1, s0.* FROM temp_itm_thread_pools s0 ) t2 ON t1.cfghostname = t2.node_name AND t1.d_timestamp0 = t2.thp_dwritetime_format1 LEFT JOIN ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS dcp_dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dcp_dwritetime_format1, s0.* FROM temp_itm_db_connection_pools s0 ) t3 ON t1.cfghostname = t3.node_name AND t1.d_timestamp0 = t3.dcp_dwritetime_format1 LEFT JOIN ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS aps_dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS aps_dwritetime_format1, s0.* FROM temp_itm_application_server s0 ) t4 ON t1.cfghostname = t4.node_name AND t1.d_timestamp0 = t4.aps_dwritetime_format1 WHERE t1.cfgServerType = 'isWAS'")
      /*模型预测 gaacIsWAS.map()*/
      val rdd2 = gaacIsWAS.map(("iswas",_))

      rdd1.union(rdd2)

      /*cfgServerType = 'isMQ'  apptrans.gaac itm.unix_memory itm.system itm.queue_data */
      val gaacIsMQ = hiveContext.sql("SELECT t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.d_timestamp0, t1.d_timestamp1, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t1.cfghostname, t1.cfgserverip, t1.cfgservertype, t1.dwritetime_format0, t1.dwritetime_format1, t1.tmzdiff, t1.dwritetime, t1.system_name, t1.dtimestamp, t1.total_virtual_storage_mb, t1.used_virtual_storage_mb, t1.avail_virtual_storage_mb, t1.virtual_storage_pct_used, t1.virtual_storage_pct_avail, t1.total_swap_space_mb, t1.used_swap_space_mb, t1.avail_swap_space_mb, t1.used_swap_space_pct, t1.avail_swap_space_pct, t1.total_real_mem_mb, t1.used_real_mem_mb, t1.avail_real_mem_mb, t1.used_real_mem_pct, t1.avail_real_mem_pct, t1.um_page_faults, t1.um_page_reclaims, t1.um_page_ins, t1.um_page_outs, t1.page_in_reqs, t1.page_out_reqs, t1.page_in_kb_s, t1.page_out_kb_s, t1.page_in_1min, t1.page_in_5min, t1.page_in_15min, t1.page_in_60min, t1.page_out_1min, t1.page_out_5min, t1.page_out_15min, t1.page_out_60min, t1.page_scan, t1.page_scan_kb, t1.page_scan_1min, t1.page_scan_5min, t1.page_scan_15min, t1.page_scan_60min, t1.arc_size_mb, t1.net_memory_used, t1.net_memory_avail, t1.net_memory_used_pct, t1.net_memory_avail_pct, t1.non_comp_memory, t1.comp_memory, t1.decay_rate, t1.repaging_rate, t1.pages_read_per_sec, t1.pages_written_per_sec, t1.paging_space_free_pct, t1.paging_space_used_pct, t1.paging_space_read_per_sec, t1.paging_space_write_per_sec, t1.comp_mem_pct, t1.non_comp_mem_pct, t1.filesys_avail_mem_pct, t1.memav_whsc, t1.rmava_whsc, t1.rmusd_whsc, t1.memus_whsc, t1.non_comp_mem_mb, t1.comp_mem_mb, t1.filesys_avail_mem_mb, t1.process_mem_pct, t1.prmep_whsc, t1.system_mem_pct, t1.symep_whsc, t1.file_repl_mem_pct, t1.frmep_whsc, t1.file_repl_min_mem_pct, t1.frmip_whsc, t1.file_repl_max_mem_pct, t1.frmap_whsc, t1.type, t1.version, t1.total_real_memory, t1.total_virtual_memory, t1.up_time, t1.users_session_number, t1.system_procs_number, t1.net_address, t1.user_cpu, t1.system_cpu, t1.idle_cpu, t1.wait_io, t1.processes_in_run_queue, t1.processes_waiting, t1.sys_page_faults, t1.sys_page_reclaims, t1.pages_paged_in, t1.pages_paged_out, t1.sys_page_ins, t1.sys_page_outs, t1.free_memory, t1.active_virtual_memory, t1.cpu_context_switches, t1.system_calls, t1.forks_executed, t1.execs_executed, t1.block_reads, t1.block_writes, t1.logical_block_reads, t1.logical_block_writes, t1.nonblock_reads, t1.nonblock_writes, t1.receive_interrupts, t1.transmit_interrupts, t1.modem_interrupts, t1.active_internet_connections, t1.active_sockets, t1.load_average_1_min, t1.load_average_5_min, t1.load_average_15_min, t1.dummy_memory_free, t1.memory_used, t1.page_scan_rate, t1.virtual_memory_percent_used, t1.vmfreeprc, t1.cpu_busy, t1.system_read, t1.system_write, t1.system_threads, t1.processes_runnable, t1.processes_running, t1.processes_sleeping, t1.processes_idle, t1.processes_zombie, t1.processes_stopped, t1.threads_in_run_queue, t1.threads_waiting, t1.boot_time, t1.pending_io_waits, t1.start_io, t1.device_interrupts, t1.uptime, t1.swap_space_free, t1.page_ins_rate, t1.page_out_rate, t1.page_scanning, t1.avg_pageins_1, t1.avg_pageins_5, t1.avg_pageins_15, t1.avg_pageins_60, t1.avg_pageout_1, t1.avg_pageout_5, t1.avg_pageout_15, t1.avg_pageout_60, t1.avg_pagescan_1, t1.avg_pagescan_5, t1.avg_pagescan_15, t1.avg_pagescan_60, t1.avg_processes_runqueue_60, t1.ipv6_address, t1.zone_id, t1.zone_name, t1.system_software_version, t1.number_of_cpus, t1.physical_consumption, t1.stolen_idle_cycles_pct, t1.stolen_busy_cycles_pct, t1.time_spent_in_hypervisor_pct, t1.bread_whsc, t1.bwrit_whsc, t1.cpubu_whsc, t1.unixi_whsc, t1.phrea_whsc, t1.phwri_whsc, t1.pc_whsc, t1.unixs_whsc, t1.unixu_whsc, t1.vmuse_whsc, t2.qd_dwritetime_format0, t2.qd_dwritetime_format1, t2.dwritetime AS qd_dwritetime, t2.origin_node AS qd_origin_node, t2.mq_manager_name, t2.queue_name, t2.host_name, t2.create_date_and_time, t2.queue_type, t2.queue_usage, t2.put_status, t2.definition_type, t2.default_persistence, t2.default_priority, t2.cluster, t2.cluster_namelist, t2.cluster_queue_manager, t2.cluster_queue_type, t2.alter_date_and_time, t2.cluster_date_and_time, t2.qsg_name, t2.cf_structure_name, t2.qsg_disposition, t2.queue_description_u, t2.current_depth, t2.input_opens, t2.output_opens, t2.total_opens, t2.trigger_control, t2.trigger_type, t2.trigger_depth, t2.trigger_priority, t2.get_status, t2.high_depth_threshold_percent, t2.percent_full, t2.maximum_depth, t2.target_or_remote_queue, t2.remote_queue_manager FROM temp_gaac_cfg_itm_base t1 LEFT JOIN ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS qd_dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS qd_dwritetime_format1, s0.* FROM temp_itm_queue_data s0 ) t2 ON t1.cfghostname = t2.host_name AND t1.d_timestamp0 = t2.qd_dwritetime_format1 WHERE t1.cfgServerType = 'isMQ'")
      /*模型预测 gaacIsMQ.map()*/


      /*cfgServerType = 'OTHER' apptrans.gaac itm.unix_memory itm.system */
      val gaacIsOTHER = hiveContext.sql("SELECT t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.d_timestamp0, t1.d_timestamp1, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t1.cfghostname, t1.cfgserverip, t1.cfgservertype, t1.dwritetime_format0, t1.dwritetime_format1, t1.tmzdiff, t1.dwritetime, t1.system_name, t1.dtimestamp, t1.total_virtual_storage_mb, t1.used_virtual_storage_mb, t1.avail_virtual_storage_mb, t1.virtual_storage_pct_used, t1.virtual_storage_pct_avail, t1.total_swap_space_mb, t1.used_swap_space_mb, t1.avail_swap_space_mb, t1.used_swap_space_pct, t1.avail_swap_space_pct, t1.total_real_mem_mb, t1.used_real_mem_mb, t1.avail_real_mem_mb, t1.used_real_mem_pct, t1.avail_real_mem_pct, t1.um_page_faults, t1.um_page_reclaims, t1.um_page_ins, t1.um_page_outs, t1.page_in_reqs, t1.page_out_reqs, t1.page_in_kb_s, t1.page_out_kb_s, t1.page_in_1min, t1.page_in_5min, t1.page_in_15min, t1.page_in_60min, t1.page_out_1min, t1.page_out_5min, t1.page_out_15min, t1.page_out_60min, t1.page_scan, t1.page_scan_kb, t1.page_scan_1min, t1.page_scan_5min, t1.page_scan_15min, t1.page_scan_60min, t1.arc_size_mb, t1.net_memory_used, t1.net_memory_avail, t1.net_memory_used_pct, t1.net_memory_avail_pct, t1.non_comp_memory, t1.comp_memory, t1.decay_rate, t1.repaging_rate, t1.pages_read_per_sec, t1.pages_written_per_sec, t1.paging_space_free_pct, t1.paging_space_used_pct, t1.paging_space_read_per_sec, t1.paging_space_write_per_sec, t1.comp_mem_pct, t1.non_comp_mem_pct, t1.filesys_avail_mem_pct, t1.memav_whsc, t1.rmava_whsc, t1.rmusd_whsc, t1.memus_whsc, t1.non_comp_mem_mb, t1.comp_mem_mb, t1.filesys_avail_mem_mb, t1.process_mem_pct, t1.prmep_whsc, t1.system_mem_pct, t1.symep_whsc, t1.file_repl_mem_pct, t1.frmep_whsc, t1.file_repl_min_mem_pct, t1.frmip_whsc, t1.file_repl_max_mem_pct, t1.frmap_whsc, t1.type, t1.version, t1.total_real_memory, t1.total_virtual_memory, t1.up_time, t1.users_session_number, t1.system_procs_number, t1.net_address, t1.user_cpu, t1.system_cpu, t1.idle_cpu, t1.wait_io, t1.processes_in_run_queue, t1.processes_waiting, t1.sys_page_faults, t1.sys_page_reclaims, t1.pages_paged_in, t1.pages_paged_out, t1.sys_page_ins, t1.sys_page_outs, t1.free_memory, t1.active_virtual_memory, t1.cpu_context_switches, t1.system_calls, t1.forks_executed, t1.execs_executed, t1.block_reads, t1.block_writes, t1.logical_block_reads, t1.logical_block_writes, t1.nonblock_reads, t1.nonblock_writes, t1.receive_interrupts, t1.transmit_interrupts, t1.modem_interrupts, t1.active_internet_connections, t1.active_sockets, t1.load_average_1_min, t1.load_average_5_min, t1.load_average_15_min, t1.dummy_memory_free, t1.memory_used, t1.page_scan_rate, t1.virtual_memory_percent_used, t1.vmfreeprc, t1.cpu_busy, t1.system_read, t1.system_write, t1.system_threads, t1.processes_runnable, t1.processes_running, t1.processes_sleeping, t1.processes_idle, t1.processes_zombie, t1.processes_stopped, t1.threads_in_run_queue, t1.threads_waiting, t1.boot_time, t1.pending_io_waits, t1.start_io, t1.device_interrupts, t1.uptime, t1.swap_space_free, t1.page_ins_rate, t1.page_out_rate, t1.page_scanning, t1.avg_pageins_1, t1.avg_pageins_5, t1.avg_pageins_15, t1.avg_pageins_60, t1.avg_pageout_1, t1.avg_pageout_5, t1.avg_pageout_15, t1.avg_pageout_60, t1.avg_pagescan_1, t1.avg_pagescan_5, t1.avg_pagescan_15, t1.avg_pagescan_60, t1.avg_processes_runqueue_60, t1.ipv6_address, t1.zone_id, t1.zone_name, t1.system_software_version, t1.number_of_cpus, t1.physical_consumption, t1.stolen_idle_cycles_pct, t1.stolen_busy_cycles_pct, t1.time_spent_in_hypervisor_pct, t1.bread_whsc, t1.bwrit_whsc, t1.cpubu_whsc, t1.unixi_whsc, t1.phrea_whsc, t1.phwri_whsc, t1.pc_whsc, t1.unixs_whsc, t1.unixu_whsc, t1.vmuse_whsc, t1.noc_whsc FROM temp_gaac_cfg_itm_base t1 WHERE t1.cfgServerType = 'OTHER'")
      /*模型预测 gaacIsOTHER.map()*/



      logger.warn("====================== TEST SHOW ==================")
      logger.warn("========== cfgServerType = 'isDB2'")
      gaacIsDB2.show()
      logger.warn("========== cfgServerType = 'isWAS'")
      gaacIsWAS.show()
      logger.warn("========== cfgServerType = 'isMQ'")
      gaacIsMQ.show()
      logger.warn("========== cfgServerType = 'OTHER'")
      gaacIsOTHER.show()


    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 获取mysql数据库中的某张表的dataframe
    *
    * @param hiveContext
    * @param props
    * @param dbtable
    * @return
    */
  def getHostConfig(hiveContext: HiveContext, props: Properties, dbtable: String): DataFrame = {
    val url = props.getProperty("mysql.db.url")
    val user = props.getProperty("mysql.db.user")
    val password = props.getProperty("mysql.db.pwd")

    val jdbcDF = hiveContext.read
      .format("jdbc")
      .options(Map("url" -> url,
        "user" -> user,
        "password" -> password,
        "dbtable" -> dbtable))
      .load()
    jdbcDF
  }

  /**
    * 区分数据类型过滤出对应的数据文件dStream
    *
    * @param map
    * @param tableName
    * @return
    */
  def checkContains(map: scala.collection.mutable.Map[String, Any], tableName: String, props: Properties): Boolean = {
    val itmFile = props.getProperty("data.itm.key")
    val itmBody = props.getProperty("data.itm.value")
    logger.warn("----------------" + map)
    logger.warn("***** " + itmFile + " : " + itmBody + " *****")
    if (map.contains(itmFile) && map.contains(itmBody)) {
      val fileName = map(itmFile).toString.toLowerCase
      if (fileName.contains(tableName)) return true
    }
    false

  }

  /**
    * 获取hive表的columns，形成schema
    *
    * @param hiveContext
    * @param tabName 表名，如：itm.system/opm.opm_db
    * @return 返回生成的schema
    */
  def getColumns(hiveContext: HiveContext, tabName: String): StructType = {
    val df = hiveContext.sql("select * from " + tabName + " limit 0")
    val columnsArr = df.columns
    val schema = StructType(columnsArr.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    schema
  }

}
