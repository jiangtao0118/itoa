package com.wisdom.spark.streaming.thread

import java.util
import java.util.Properties

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil, SysConst}
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.etl.util.InitUtil
import com.wisdom.spark.streaming.service.PredResultService
import com.wisdom.spark.streaming.tools.JsonUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * 系统响应时间模型预测分析
  * Created by zhengz on 2017/2/14.
  * 步骤：
  * 1.创建streaming-Context，采集周期1分钟
  * 2.Topic-APPTRANS消息接收DSm-APPTRANS
  * 3.Topic-OPM消息接收DSm-OPM
  * 4.Topic-ITM消息接收DSm-ITM
  * 5.针对ITM的DSm Window操作，滑块设置1分钟；窗口设置5分钟，得到新 DSm
  * 6.连接MySQL获取主机和IP对应关系，DSM
  * 7.将多个DSM进行union，并作foreachRDD处理，获得RDD  {@filename:dddd ,@message:dddd}
  * 8.过滤rdd数据，分割为不同文件的rdd
  * 9.获取hive中表字段信息作为schema
  * 10.将rdd转换为dataframe
  * 11.将dataframe注册到表
  * 12.表与表进行关联，进行条件筛选、数据筛选
  * 13.再转会rdd，作map操作得到元组数据("文件名或表名或模型类别","逗号分隔的数据")
  * 14.map调用海涛的模型预测接口返回预测结果
  * 15.预测结果toList集合保存mysql数据库
  *
  */
object Thread4SysResponseTime {
  @transient
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val sparkContext = SparkContextUtil.getInstance()
    val hiveContext = new HiveContext(sparkContext)
    val props = ItoaPropertyUtil.getProperties()
    val modelObj = Thread4InitialModelObj.getModelMap()

    /** 获取主机IP配置信息 */
    val cfgTabNme = props.getProperty("df_tab_cfg_name")
    val cfgSourceDF = getHostConfig(hiveContext, props, cfgTabNme)
    cfgSourceDF.registerTempTable("temp_source_conf_info")
    val cfgDF = hiveContext.sql("SELECT DISTINCT trim(hostName) AS cfgHostName, trim(serverIP) AS cfgServerIP, ( CASE WHEN isDB2 IN ('YES', 'Y') THEN 'isDB2' WHEN isWAS IN ('YES', 'Y') THEN 'isWAS' WHEN isMQ IN ('YES', 'Y') THEN 'isMQ' ELSE 'OTHER' END ) AS cfgServerType FROM temp_source_conf_info")
    cfgDF.registerTempTable("temp_host_conf_info")

    /** kafka参数 */
    val brokers = props.getProperty("kafka.common.brokers")
    //    val offset = props.getProperty("kafka.param.offset.value")


    /** 广播变量 */
    val initial = new InitUtil(props, modelObj)
    val initialBroadCast = sparkContext.broadcast(initial)


    /** 获取相关HIVE表字段信息，生成schema 表名需要根据实际情况进行修改，后期IBM会将表由外表转内表，表名可能存在不一致的情况，需变更 */
    val apptransCols = getColumns(hiveContext, "apptrans.gaac") //由于apptarans中五张表表结构一致，所以只需要查询一张表即可
    val opmDbCols = getColumns(hiveContext, "opm.opm_db")
    val unixMemoryCols = getColumns(hiveContext, "itm.unix_memory")
    val systemCols = getColumns(hiveContext, "itm.system")
    val threadPoolsCols = getColumns(hiveContext, "itm.thread_pools")
    val dbConnectionPoolsCols = getColumns(hiveContext, "itm.db_connection_pools")
    val applicationServerCols = getColumns(hiveContext, "itm.application_server")
    val queueDataCols = getColumns(hiveContext, "itm.queue_data")

    /** 重新分区个数 */
    val partitions = props.getProperty("kafka.data.partitions").toInt

    val sInterval = props.getProperty("gaac.kafka.stream.interval").toInt
    val itmTopic = props.getProperty("gaac.kafka.topic.itm").split(",").toSet
    val opmTopic = props.getProperty("gaac.kafka.topic.opm").split(",").toSet
    val apptransTopic = props.getProperty("gaac.kafka.topic.apptrans").split(",").toSet
    val kafkaParams = Map[String, String](props.getProperty("kafka.param.brokers.key") -> brokers) //, props.getProperty("kafka.param.offset.key") -> offset)

    /** streaming监听周期1分钟  以apptrans为主 **/
    val ssc = new StreamingContext(sparkContext, Seconds(sInterval))

    /** 定义变量==>数据接收时间 var dataGetTime = -1L **/
    var dataGetTime = -1L
    /** 配置文件获取key键 **/
    val itmFile = props.getProperty("data.itm.key")
    val itmBody = props.getProperty("data.itm.value")
    /** 修改  APPTRANS 1分钟  ITM/OPM 增加分钟窗口 需再次确认ITM数据OPM数据APPTRANS数据Kafka发送间隔 **/
    val itmDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, itmTopic).map(_._2).window(Seconds(sInterval * 5), Seconds(sInterval))
    val opmDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, opmTopic).map(_._2).window(Seconds(sInterval * 5), Seconds(sInterval))
    val apptransDSM = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, apptransTopic).map(_._2)


    /** 对DSM作transform操作 **/
    val finalItmDSM = itmDSM.transform(trans => {
      dataGetTime = System.currentTimeMillis() / 1000 //时间格式还未定制，暂时默认
      trans.distinct().repartition(partitions)
    }).map(JsonUtil.parseJSON(_)).filter(map => {
      if (map.contains(itmFile) && map.contains(itmBody)) true else false
    }).map(map => (map(itmBody).toString, map(itmFile).toString)).transform(trans => {
      trans.sortByKey(true)
    })
    val finalOpmDSM = opmDSM.transform(trans => {
      trans.distinct().repartition(partitions)
    }).map(JsonUtil.parseJSON(_)).filter(map => {
      if (map.contains(itmFile) && map.contains(itmBody)) true else false
    }).map(map => (map(itmBody).toString, map(itmFile).toString))
    val finalApsDSM = apptransDSM.transform(trans => {
      trans.distinct().repartition(partitions)
    }).map(JsonUtil.parseJSON(_)).filter(map => {
      if (map.contains(itmFile) && map.contains(itmBody)) true else false
    }).map(map => (map(itmBody).toString, map(itmFile).toString))
    /** DSM合并作RDD操作 **/

    val allDSM = finalItmDSM.union(finalOpmDSM).union(finalApsDSM)


    allDSM.foreachRDD(rdd => {
      /** 获取广播变量 **/
      val initial = initialBroadCast.value
      val rddProps = initial.properties
      /** 配置文件中获取临时表文件名区分标志 **/
      val ecupinFile = rddProps.getProperty("df_tab_file_ecupin")
      val ecupoutFile = rddProps.getProperty("df_tab_file_ecupout")
      val gaacFile = rddProps.getProperty("df_tab_file_gaac")
      val mobsFile = rddProps.getProperty("df_tab_file_mobs")
      val pbankFile = rddProps.getProperty("df_tab_file_pbank")
      val odbFile = rddProps.getProperty("df_tab_file_opmdb")
      val sysFile = rddProps.getProperty("df_tab_file_system")
      val umFile = rddProps.getProperty("df_tab_file_unixmemory")
      val thpFile = rddProps.getProperty("df_tab_file_threadpools")
      val dcpFile = rddProps.getProperty("df_tab_file_dbconnectionpools")
      val apsFile = rddProps.getProperty("df_tab_file_applicationserver")
      val qdFile = rddProps.getProperty("df_tab_file_queuedata")

      /** 将rdd根据文件名进行过滤拆分为多个rdd */
      val ecupinRDD = rdd.filter(_._2.toLowerCase.contains(ecupinFile))
      val ecupoutRDD = rdd.filter(_._2.toLowerCase.contains(ecupoutFile))
      val gaacRDD = rdd.filter(_._2.toLowerCase.contains(gaacFile))
      val mobsRDD = rdd.filter(_._2.toLowerCase.contains(mobsFile))
      val pbankRDD = rdd.filter(_._2.toLowerCase.contains(pbankFile))
      val odbRDD = rdd.filter(_._2.toLowerCase.contains(odbFile))
      val sysRDD = rdd.filter(_._2.toLowerCase.contains(sysFile))
      val umRDD = rdd.filter(_._2.toLowerCase.contains(umFile))
      val thpRDD = rdd.filter(_._2.toLowerCase.contains(thpFile))
      val dcpRDD = rdd.filter(_._2.toLowerCase.contains(dcpFile))
      val apsRDD = rdd.filter(_._2.toLowerCase.contains(apsFile))
      val qdRDD = rdd.filter(_._2.toLowerCase.contains(qdFile))

      /** apptrans RDD合并 (gaac、mobs、pbank合并，结构相同且不重复，合并一张表，方便注册一张临时表；ecupin和ecupout存在主机重复，则需要join操作) */
      val aptsRDD = gaacRDD.union(mobsRDD).union(pbankRDD)

      /** 将RDD映射到dataframe；部分表依据业务需求需作数据筛选，例如gaac等 **/
      val ecupinDF = hiveContext.createDataFrame(ecupinRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), apptransCols).filter(" metric_name = '服务器IP' ")
      val ecupoutDF = hiveContext.createDataFrame(ecupoutRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), apptransCols).filter(" metric_name = '服务器IP' ")
      val aptsDF = hiveContext.createDataFrame(aptsRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), apptransCols).filter(" metric_name = '服务器IP' ")
      val opmDbDF = hiveContext.createDataFrame(odbRDD.map(m => Row.fromSeq(m._1.split("\\^").toSeq)), opmDbCols)
      val systemDF = hiveContext.createDataFrame(sysRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), systemCols)
      val unixMemoryDF = hiveContext.createDataFrame(umRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), unixMemoryCols)
      val threadPoolsDF = hiveContext.createDataFrame(thpRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), threadPoolsCols)
      val dbConnectionPoolsDF = hiveContext.createDataFrame(dcpRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), dbConnectionPoolsCols)
      val applicationServerDF = hiveContext.createDataFrame(apsRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), applicationServerCols)
      val queueDataDF = hiveContext.createDataFrame(qdRDD.map(m => Row.fromSeq(m._1.split(",").toSeq)), queueDataCols)

      /** 将dataframe注册到临时表 */
      ecupinDF.registerTempTable("temp_apptrans_ecupin")
      ecupoutDF.registerTempTable("temp_apptrans_ecupout")
      aptsDF.registerTempTable("temp_apptrans_gmp")
      opmDbDF.registerTempTable("temp_opm_opm_db")
      unixMemoryDF.registerTempTable("temp_itm_unix_memory")
      systemDF.registerTempTable("temp_itm_system")
      threadPoolsDF.registerTempTable("temp_itm_thread_pools")
      dbConnectionPoolsDF.registerTempTable("temp_itm_db_connection_pools")
      applicationServerDF.registerTempTable("temp_itm_application_server")
      queueDataDF.registerTempTable("temp_itm_queue_data")

      /** 数据处理：时间字段转换、主机字段截取、重复去除、条件筛选等 **/

      /** base_apptrans_ecup:ECUP两张表IN和OUT实现JOIN操作转换为宽表，加字段前缀区分，时间字段加工处理，以及与主机配置表JOIN获取主机类型信息，作为基础表 */
      val ecupCfgDF = hiveContext.sql("SELECT DISTINCT trim( regexp_replace ( concat(t1.d_date, t1.d_time), '-|:', '' )) AS d_timestamp0, trim( regexp_replace ( concat( t1.d_date, substring(trim(t1.d_time), 1, 4), ( CASE WHEN substring(trim(t1.d_time), 5, 1) < '5' THEN '0' ELSE '5' END )), '-|:', '' )) AS d_timestamp1, t1.metric_name AS in_metric_name, t1.metric_value AS in_metric_value, t1.d_date AS in_d_date, t1.d_time AS in_d_time, t1.trans_volume AS in_trans_volume, t1.trans_rate AS in_trans_rate, t1.avg_trans_time AS in_avg_trans_time, t2.metric_name AS out_metric_name, t2.metric_value AS out_metric_value, t2.d_date AS out_d_date, t2.d_time AS out_d_time, t2.trans_volume AS out_trans_volume, t2.trans_rate AS out_trans_rate, t2.avg_trans_time AS out_avg_trans_time, t3.cfgHostName, t3.cfgServerIP, t3.cfgServerType FROM temp_apptrans_ecupin t1 LEFT JOIN temp_apptrans_ecupout t2 ON t1.metric_name = t2.metric_name AND t1.metric_value = t2.metric_value AND t1.d_date = t2.d_date AND t1.d_time = t2.d_time LEFT JOIN temp_host_conf_info t3 ON trim(t1.metric_value) = t3.cfgServerIP")
      ecupCfgDF.registerTempTable("base_apptrans_ecup")

      /** base_apptrans_gmp:GAAC/MOBS/PBANK合并后的DF，进行时间字段加工处理，与主机配置JOIN获取主机信息，作为基础表 */
      val aptsCfgDF = hiveContext.sql("SELECT DISTINCT trim( regexp_replace ( concat(t1.d_date, t1.d_time), '-|:', '' )) AS d_timestamp0, trim( regexp_replace ( concat( t1.d_date, substring(trim(t1.d_time), 1, 4), ( CASE WHEN substring(trim(t1.d_time), 5, 1) < '5' THEN '0' ELSE '5' END )), '-|:', '' )) AS d_timestamp1, t1.metric_name, t1.metric_value, t1.d_date, t1.d_time, t1.trans_volume, t1.trans_rate, t1.avg_trans_time, t2.cfghostname, t2.cfgserverip, t2.cfgservertype FROM temp_apptrans_gmp t1 LEFT JOIN temp_host_conf_info ON t1.metric_value = t2.cfgServerIP")
      aptsCfgDF.registerTempTable("base_apptrans_gmp")

      /** base_opm_db:OPMDB 进行时间字段处理，以及数据去除重复值，作为基础表 */
      val odbDF = hiveContext.sql("SELECT s.opmtime_format0 AS odb_opmtime_format0, s.opmtime_format1 AS odb_opmtime_format1, s.opm_db_host_name AS odb_opm_db_host_name, max(s.opm_db_name) AS odb_opm_db_name, max( s.opm_min_collection_timestamp ) AS odb_opm_min_collection_timestamp, max(s.db_status) AS odb_db_status, max(s.appls_cur_cons) AS odb_appls_cur_cons, max(s.appls_in_db2) AS odb_appls_in_db2, max(s.total_log_used) AS odb_total_log_used, max(s.total_log_available) AS odb_total_log_available, max(s.lock_list_in_use) AS odb_lock_list_in_use, max(s.lock_escals) AS odb_lock_escals, max(s.lock_timeouts) AS odb_lock_timeouts, max(s.locks_waiting) AS odb_locks_waiting, max(s.deadlocks) AS odb_deadlocks, max(s.opm_transactions) AS odb_opm_transactions, max(s.lock_wait_time) AS odb_lock_wait_time, max(s.opm_sort_time_per_sort) AS odb_opm_sort_time_per_sort, max( s.opm_sorts_per_transaction ) AS odb_opm_sorts_per_transaction, max(s.sort_overflows) AS odb_sort_overflows, max(s.rows_read) AS odb_rows_read, max(s.opm_logical_reads) AS odb_opm_logical_reads, max(s.pool_async_index_reads) AS odb_pool_async_index_reads, max(s.opm_bp_hitratio) AS odb_opm_bp_hitratio, max(s.opm_log_buffer_hitratio) AS odb_opm_log_buffer_hitratio, max( s.opm_log_read_time_per_log_read ) AS odb_opm_log_read_time_per_log_read, max( s.opm_log_write_time_per_log_write ) AS odb_opm_log_write_time_per_log_write, max(s.num_log_read_io) AS odb_num_log_read_io, max(s.num_log_write_io) AS odb_num_log_write_io, max(s.log_disk_wait_time) AS odb_log_disk_wait_time, max(s.log_write_time) AS odb_log_write_time, max( s.opm_pool_read_time_per_read ) AS odb_opm_pool_read_time_per_read, max( s.opm_pool_write_time_per_write ) AS odb_opm_pool_write_time_per_write FROM ( SELECT r0.opmtime_format0, concat( substring(r0.opmtime_format0, 1, 11), ( CASE WHEN substring(r0.opmtime_format0, 12, 1) < '5' THEN '0' ELSE '5' END )) AS opmtime_format1, r0.* FROM ( SELECT substring( regexp_replace ( s0.opm_min_collection_timestamp, '-|:|T', '' ), 1, 12 ) AS opmtime_format0, s0.* FROM temp_opm_opm_db s0 ) r0 ) s GROUP BY s.opmtime_format0, s.opmtime_format1, s.opm_db_host_name")
      odbDF.registerTempTable("base_opm_db")

      /** base_itm_um:ITM.UNIX_MEMORY 时间字段处理，主机名截取，数据去除重复值，作为基础表 */
      val umDF = hiveContext.sql("SELECT max(s.dwritetime_format0) AS um_dwritetime_format0, s.dwritetime_format1 AS um_dwritetime_format1, s.host_format AS um_host_format, max(s.tmzdiff) AS um_tmzdiff, max(s.dwritetime) AS um_dwritetime, max(s.system_name) AS um_system_name, max(s.dtimestamp) AS um_dtimestamp, max(s.total_virtual_storage_mb) AS um_total_virtual_storage_mb, max(s.used_virtual_storage_mb) AS um_used_virtual_storage_mb, max(s.avail_virtual_storage_mb) AS um_avail_virtual_storage_mb, max(s.virtual_storage_pct_used) AS um_virtual_storage_pct_used, max(s.virtual_storage_pct_avail) AS um_virtual_storage_pct_avail, max(s.total_swap_space_mb) AS um_total_swap_space_mb, max(s.used_swap_space_mb) AS um_used_swap_space_mb, max(s.avail_swap_space_mb) AS um_avail_swap_space_mb, max(s.used_swap_space_pct) AS um_used_swap_space_pct, max(s.avail_swap_space_pct) AS um_avail_swap_space_pct, max(s.total_real_mem_mb) AS um_total_real_mem_mb, max(s.used_real_mem_mb) AS um_used_real_mem_mb, max(s.avail_real_mem_mb) AS um_avail_real_mem_mb, max(s.used_real_mem_pct) AS um_used_real_mem_pct, max(s.avail_real_mem_pct) AS um_avail_real_mem_pct, max(s.page_faults) AS um_page_faults, max(s.page_reclaims) AS um_page_reclaims, max(s.page_ins) AS um_page_ins, max(s.page_outs) AS um_page_outs, max(s.page_in_reqs) AS um_page_in_reqs, max(s.page_out_reqs) AS um_page_out_reqs, max(s.page_in_kb_s) AS um_page_in_kb_s, max(s.page_out_kb_s) AS um_page_out_kb_s, max(s.page_in_1min) AS um_page_in_1min, max(s.page_in_5min) AS um_page_in_5min, max(s.page_in_15min) AS um_page_in_15min, max(s.page_in_60min) AS um_page_in_60min, max(s.page_out_1min) AS um_page_out_1min, max(s.page_out_5min) AS um_page_out_5min, max(s.page_out_15min) AS um_page_out_15min, max(s.page_out_60min) AS um_page_out_60min, max(s.page_scan) AS um_page_scan, max(s.page_scan_kb) AS um_page_scan_kb, max(s.page_scan_1min) AS um_page_scan_1min, max(s.page_scan_5min) AS um_page_scan_5min, max(s.page_scan_15min) AS um_page_scan_15min, max(s.page_scan_60min) AS um_page_scan_60min, max(s.arc_size_mb) AS um_arc_size_mb, max(s.net_memory_used) AS um_net_memory_used, max(s.net_memory_avail) AS um_net_memory_avail, max(s.net_memory_used_pct) AS um_net_memory_used_pct, max(s.net_memory_avail_pct) AS um_net_memory_avail_pct, max(s.non_comp_memory) AS um_non_comp_memory, max(s.comp_memory) AS um_comp_memory, max(s.decay_rate) AS um_decay_rate, max(s.repaging_rate) AS um_repaging_rate, max(s.pages_read_per_sec) AS um_pages_read_per_sec, max(s.pages_written_per_sec) AS um_pages_written_per_sec, max(s.paging_space_free_pct) AS um_paging_space_free_pct, max(s.paging_space_used_pct) AS um_paging_space_used_pct, max(s.paging_space_read_per_sec) AS um_paging_space_read_per_sec, max(s.paging_space_write_per_sec) AS um_paging_space_write_per_sec, max(s.comp_mem_pct) AS um_comp_mem_pct, max(s.non_comp_mem_pct) AS um_non_comp_mem_pct, max(s.filesys_avail_mem_pct) AS um_filesys_avail_mem_pct, max(s.memav_whsc) AS um_memav_whsc, max(s.rmava_whsc) AS um_rmava_whsc, max(s.rmusd_whsc) AS um_rmusd_whsc, max(s.memus_whsc) AS um_memus_whsc, max(s.non_comp_mem_mb) AS um_non_comp_mem_mb, max(s.comp_mem_mb) AS um_comp_mem_mb, max(s.filesys_avail_mem_mb) AS um_filesys_avail_mem_mb, max(s.process_mem_pct) AS um_process_mem_pct, max(s.prmep_whsc) AS um_prmep_whsc, max(s.system_mem_pct) AS um_system_mem_pct, max(s.symep_whsc) AS um_symep_whsc, max(s.file_repl_mem_pct) AS um_file_repl_mem_pct, max(s.frmep_whsc) AS um_frmep_whsc, max(s.file_repl_min_mem_pct) AS um_file_repl_min_mem_pct, max(s.frmip_whsc) AS um_frmip_whsc, max(s.file_repl_max_mem_pct) AS um_file_repl_max_mem_pct, max(s.frmap_whsc) AS um_frmap_whsc FROM ( SELECT concat('20', substring(s1.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s1.dwritetime, 2, 9), ( CASE WHEN substring(s1.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, split (s1.system_name, ':') [ 0 ] AS host_format, s1.* FROM temp_itm_unix_memory s1 ) s GROUP BY s.dwritetime_format1, s.host_format")
      umDF.registerTempTable("base_itm_um")

      /** base_itm_sys:ITM.SYSTEM 时间字段处理，主机名截取，数据去除重复值，作为基础表 */
      val sysDF = hiveContext.sql("SELECT max(s.dwritetime_format0) AS sys_dwritetime_format0, s.dwritetime_format1 AS sys_dwritetime_format1, s.host_format AS sys_host_format, max(s.tmzdiff) AS sys_tmzdiff, max(s.dwritetime) AS sys_dwritetime, max(s.system_name) AS sys_system_name, max(s.dtimestamp) AS sys_dtimestamp, max(s.type) AS sys_type, max(s.version) AS sys_version, max(s.total_real_memory) AS sys_total_real_memory, max(s.total_virtual_memory) AS sys_total_virtual_memory, max(s.up_time) AS sys_up_time, max(s.users_session_number) AS sys_users_session_number, max(s.system_procs_number) AS sys_system_procs_number, max(s.net_address) AS sys_net_address, max(s.user_cpu) AS sys_user_cpu, max(s.system_cpu) AS sys_system_cpu, max(s.idle_cpu) AS sys_idle_cpu, max(s.wait_io) AS sys_wait_io, max(s.processes_in_run_queue) AS sys_processes_in_run_queue, max(s.processes_waiting) AS sys_processes_waiting, max(s.page_faults) AS sys_page_faults, max(s.page_reclaims) AS sys_page_reclaims, max(s.pages_paged_in) AS sys_pages_paged_in, max(s.pages_paged_out) AS sys_pages_paged_out, max(s.page_ins) AS sys_page_ins, max(s.page_outs) AS sys_page_outs, max(s.free_memory) AS sys_free_memory, max(s.active_virtual_memory) AS sys_active_virtual_memory, max(s.cpu_context_switches) AS sys_cpu_context_switches, max(s.system_calls) AS sys_system_calls, max(s.forks_executed) AS sys_forks_executed, max(s.execs_executed) AS sys_execs_executed, max(s.block_reads) AS sys_block_reads, max(s.block_writes) AS sys_block_writes, max(s.logical_block_reads) AS sys_logical_block_reads, max(s.logical_block_writes) AS sys_logical_block_writes, max(s.nonblock_reads) AS sys_nonblock_reads, max(s.nonblock_writes) AS sys_nonblock_writes, max(s.receive_interrupts) AS sys_receive_interrupts, max(s.transmit_interrupts) AS sys_transmit_interrupts, max(s.modem_interrupts) AS sys_modem_interrupts, max(s.active_internet_connections) AS sys_active_internet_connections, max(s.active_sockets) AS sys_active_sockets, max(s.load_average_1_min) AS sys_load_average_1_min, max(s.load_average_5_min) AS sys_load_average_5_min, max(s.load_average_15_min) AS sys_load_average_15_min, max(s.dummy_memory_free) AS sys_dummy_memory_free, max(s.memory_used) AS sys_memory_used, max(s.page_scan_rate) AS sys_page_scan_rate, max(s.virtual_memory_percent_used) AS sys_virtual_memory_percent_used, max(s.vmfreeprc) AS sys_vmfreeprc, max(s.cpu_busy) AS sys_cpu_busy, max(s.system_read) AS sys_system_read, max(s.system_write) AS sys_system_write, max(s.system_threads) AS sys_system_threads, max(s.processes_runnable) AS sys_processes_runnable, max(s.processes_running) AS sys_processes_running, max(s.processes_sleeping) AS sys_processes_sleeping, max(s.processes_idle) AS sys_processes_idle, max(s.processes_zombie) AS sys_processes_zombie, max(s.processes_stopped) AS sys_processes_stopped, max(s.threads_in_run_queue) AS sys_threads_in_run_queue, max(s.threads_waiting) AS sys_threads_waiting, max(s.boot_time) AS sys_boot_time, max(s.pending_io_waits) AS sys_pending_io_waits, max(s.start_io) AS sys_start_io, max(s.device_interrupts) AS sys_device_interrupts, max(s.uptime) AS sys_uptime, max(s.swap_space_free) AS sys_swap_space_free, max(s.page_ins_rate) AS sys_page_ins_rate, max(s.page_out_rate) AS sys_page_out_rate, max(s.page_scanning) AS sys_page_scanning, max(s.avg_pageins_1) AS sys_avg_pageins_1, max(s.avg_pageins_5) AS sys_avg_pageins_5, max(s.avg_pageins_15) AS sys_avg_pageins_15, max(s.avg_pageins_60) AS sys_avg_pageins_60, max(s.avg_pageout_1) AS sys_avg_pageout_1, max(s.avg_pageout_5) AS sys_avg_pageout_5, max(s.avg_pageout_15) AS sys_avg_pageout_15, max(s.avg_pageout_60) AS sys_avg_pageout_60, max(s.avg_pagescan_1) AS sys_avg_pagescan_1, max(s.avg_pagescan_5) AS sys_avg_pagescan_5, max(s.avg_pagescan_15) AS sys_avg_pagescan_15, max(s.avg_pagescan_60) AS sys_avg_pagescan_60, max(s.avg_processes_runqueue_60) AS sys_avg_processes_runqueue_60, max(s.ipv6_address) AS sys_ipv6_address, max(s.zone_id) AS sys_zone_id, max(s.zone_name) AS sys_zone_name, max(s.system_software_version) AS sys_system_software_version, max(s.number_of_cpus) AS sys_number_of_cpus, max(s.physical_consumption) AS sys_physical_consumption, max(s.stolen_idle_cycles_pct) AS sys_stolen_idle_cycles_pct, max(s.stolen_busy_cycles_pct) AS sys_stolen_busy_cycles_pct, max(s.time_spent_in_hypervisor_pct) AS sys_time_spent_in_hypervisor_pct, max(s.bread_whsc) AS sys_bread_whsc, max(s.bwrit_whsc) AS sys_bwrit_whsc, max(s.cpubu_whsc) AS sys_cpubu_whsc, max(s.unixi_whsc) AS sys_unixi_whsc, max(s.phrea_whsc) AS sys_phrea_whsc, max(s.phwri_whsc) AS sys_phwri_whsc, max(s.pc_whsc) AS sys_pc_whsc, max(s.unixs_whsc) AS sys_unixs_whsc, max(s.unixu_whsc) AS sys_unixu_whsc, max(s.vmuse_whsc) AS sys_vmuse_whsc, max(s.noc_whsc) AS sys_noc_whsc FROM ( SELECT concat('20', substring(s1.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s1.dwritetime, 2, 9), ( CASE WHEN substring(s1.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, split (s1.system_name, ':') [ 0 ] AS host_format, s1.* FROM temp_itm_system s1 ) s GROUP BY s.dwritetime_format1, s.host_format")
      sysDF.registerTempTable("base_itm_sys")

      /** base_itm_thp:ITM.THREAD_POOLS 时间字段处理， thread_pool_name = 'WebContainer'  数据筛选 ，数据去除重复值，作为基础表 */
      val thpDF = hiveContext.sql("SELECT max(s1.dwritetime_format0) AS thp_dwritetime_format0, s1.dwritetime_format1 AS thp_dwritetime_format1, max(s1.tmzdiff) AS thp_tmzdiff, max(s1.dwritetime) AS thp_dwritetime, max(s1.origin_node) AS thp_origin_node, max(s1.server_name) AS thp_server_name, s1.node_name AS thp_node_name, max(s1.sample_date_and_time) AS thp_sample_date_and_time, max(s1.interval_time) AS thp_interval_time, max(s1.thread_pool_name) AS thp_thread_pool_name, max( s1.set_instrumentation_level_type ) AS thp_set_instrumentation_level_type, max(s1.instrumentation_level) AS thp_instrumentation_level, max(s1.summary_of_thread_pools) AS thp_summary_of_thread_pools, max(s1.threads_created) AS thp_threads_created, max(s1.thread_creation_rate) AS thp_thread_creation_rate, max(s1.threads_destroyed) AS thp_threads_destroyed, max(s1.thread_destruction_rate) AS thp_thread_destruction_rate, max(s1.average_active_threads) AS thp_average_active_threads, max(s1.average_pool_size) AS thp_average_pool_size, max( s1.percent_of_time_pool_at_max ) AS thp_percent_of_time_pool_at_max, max(s1.average_free_threads) AS thp_average_free_threads, max(s1.maximum_pool_size) AS thp_maximum_pool_size, max(s1.app_id) AS thp_app_id, max(s1.percent_used_good) AS thp_percent_used_good, max(s1.percent_used_fair) AS thp_percent_used_fair, max(s1.percent_used_bad) AS thp_percent_used_bad FROM ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, s0.* FROM temp_itm_thread_pools s0 where s0.thread_pool_name = 'WebContainer' ) s1 GROUP BY s1.dwritetime_format1, s1.node_name")
      thpDF.registerTempTable("base_itm_thp")

      /** base_itm_dcp:ITM.DB_CONNECTION_POOLS 时间字段处理， datasource_name = '[Summary]'  数据筛选 ，数据去除重复值，作为基础表 */
      val dcpDF = hiveContext.sql("SELECT max(s1.dwritetime_format0) AS dcp_dwritetime_format0, s1.dwritetime_format1 AS dcp_dwritetime_format1, max(s1.tmzdiff) AS dcp_tmzdiff, max(s1.dwritetime) AS dcp_dwritetime, max(s1.origin_node) AS dcp_origin_node, max(s1.server_name) AS dcp_server_name, s1.node_name AS dcp_node_name, max(s1.sample_date_and_time) AS dcp_sample_date_and_time, max(s1.interval_time) AS dcp_interval_time, max(s1.instrumentation_level) AS dcp_instrumentation_level, max(s1.datasource_name) AS dcp_datasource_name, max(s1.connections_created) AS dcp_connections_created, max(s1.connections_destroyed) AS dcp_connections_destroyed, max(s1.connections_allocated) AS dcp_connections_allocated, max(s1.threads_timed_out) AS dcp_threads_timed_out, max(s1.average_pool_size) AS dcp_average_pool_size, max(s1.average_waiting_threads) AS dcp_average_waiting_threads, max(s1.average_wait_time) AS dcp_average_wait_time, max(s1.average_usage_time) AS dcp_average_usage_time, max(s1.percent_used) AS dcp_percent_used, max( s1.percent_of_time_pool_at_max ) AS dcp_percent_of_time_pool_at_max, max( s1.connection_creation_rate ) AS dcp_connection_creation_rate, max( s1.connection_destruction_rate ) AS dcp_connection_destruction_rate, max( s1.connection_allocation_rate ) AS dcp_connection_allocation_rate, max(s1.thread_timeout_rate) AS dcp_thread_timeout_rate, max( s1.summary_of_all_db_connections ) AS dcp_summary_of_all_db_connections, max( s1.set_instrumentation_level_type ) AS dcp_set_instrumentation_level_type, max( s1.prep_statement_cache_discards ) AS dcp_prep_statement_cache_discards, max(s1.rte_pscd) AS dcp_rte_pscd, max(s1.return_count) AS dcp_return_count, max(s1.return_rate) AS dcp_return_rate, max(s1.maximum_pool_size) AS dcp_maximum_pool_size, max(s1.datasource_label) AS dcp_datasource_label, max(s1.connection_used) AS dcp_connection_used, max(s1.total_usage_time) AS dcp_total_usage_time, max(s1.total_wait_time) AS dcp_total_wait_time, max(s1.connection_granted) AS dcp_connection_granted, max(s1.app_id) AS dcp_app_id, max(s1.percent_used_good) AS dcp_percent_used_good, max(s1.percent_used_fair) AS dcp_percent_used_fair, max(s1.percent_used_bad) AS dcp_percent_used_bad, max(s1.pool_size) AS dcp_pool_size, max(s1.average_free_pool_size) AS dcp_average_free_pool_size, max(s1.connectionhandle) AS dcp_connectionhandle, max(s1.jdbc_time) AS dcp_jdbc_time, max(s1.wait_time_count) AS dcp_wait_time_count, max(s1.usage_time_count) AS dcp_usage_time_count, max(s1.jdbc_time_count) AS dcp_jdbc_time_count FROM ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, s0.* FROM temp_itm_db_connection_pools s0 where s0.datasource_name = '[Summary]') s1 GROUP BY s1.dwritetime_format1, s1.node_name")
      dcpDF.registerTempTable("base_itm_dcp")

      /** base_itm_aps:ITM.APPLICATION_SERVER 时间字段处理，数据去除重复值，作为基础表 */
      val apsDF = hiveContext.sql("SELECT max(s1.dwritetime_format0) AS aps_dwritetime_format0, s1.dwritetime_format1 AS aps_dwritetime_format1, max(s1.tmzdiff) AS aps_tmzdiff, max(s1.dwritetime) AS aps_dwritetime, max(s1.origin_node) AS aps_origin_node, max(s1.server_name) AS aps_server_name, s1.node_name AS aps_node_name, max(s1.sample_date_and_time) AS aps_sample_date_and_time, max(s1.interval_time) AS aps_interval_time, max(s1. STATUS) AS aps_status, max(s1.version) AS aps_version, max(s1.start_date_and_time) AS aps_start_date_and_time, max(s1.process_id) AS aps_process_id, max(s1.jvm_memory_free) AS aps_jvm_memory_free, max(s1.jvm_memory_total) AS aps_jvm_memory_total, max(s1.jvm_memory_used) AS aps_jvm_memory_used, max(s1.cpu_used) AS aps_cpu_used, max(s1.cpu_used_percent) AS aps_cpu_used_percent, max(s1.instrumentation_level) AS aps_instrumentation_level, max(s1.server_type) AS aps_server_type, max(s1.server_instance_name) AS aps_server_instance_name, max( s1.request_data_monitoring_level ) AS aps_request_data_monitoring_level, max( s1.request_data_sampling_rate ) AS aps_request_data_sampling_rate, max( s1.resource_data_monitoring ) AS aps_resource_data_monitoring, max( s1.garbage_collection_monitoring ) AS aps_garbage_collection_monitoring, max(s1.system_paging_rate) AS aps_system_paging_rate, max(s1.jvm_memory_free_kb) AS aps_jvm_memory_free_kb, max(s1.jvm_memory_total_kb) AS aps_jvm_memory_total_kb, max(s1.jvm_memory_used_kb) AS aps_jvm_memory_used_kb, max(s1.platform_cpu_used) AS aps_platform_cpu_used, max(s1.asid) AS aps_asid, max(s1.summary) AS aps_summary, max(s1.ve_host_port) AS aps_ve_host_port, max(s1.probe_id) AS aps_probe_id, max(s1.server_mode) AS aps_server_mode, max(s1.server_subnode_name) AS aps_server_subnode_name, max(s1.hung_threads_blocked) AS aps_hung_threads_blocked, max(s1.hung_threads_waiting) AS aps_hung_threads_waiting, max( s1.hung_threads_timed_waiting ) AS aps_hung_threads_timed_waiting, max(s1.hung_threads_total) AS aps_hung_threads_total, max(s1.was_node_name) AS aps_was_node_name, max(s1.was_cell_name) AS aps_was_cell_name FROM ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, s0.* FROM temp_itm_application_server s0 ) s1 GROUP BY s1.dwritetime_format1, s1.node_name")
      apsDF.registerTempTable("base_itm_aps")

      /** base_itm_qd:ITM.QUEUE_DATA 时间字段处理，数据去除重复值，作为基础表 */
      val qdDF = hiveContext.sql("SELECT max(s1.dwritetime_format0) AS qd_dwritetime_format0, s1.dwritetime_format1 AS qd_dwritetime_format1, max(s1.tmzdiff) AS qd_tmzdiff, max(s1.dwritetime) AS qd_dwritetime, max(s1.origin_node) AS qd_origin_node, max(s1.mq_manager_name) AS qd_mq_manager_name, max(s1.queue_name) AS qd_queue_name, s1.host_name AS qd_host_name, max(s1.create_date_and_time) AS qd_create_date_and_time, max(s1.queue_type) AS qd_queue_type, max(s1.queue_usage) AS qd_queue_usage, max(s1.put_status) AS qd_put_status, max(s1.definition_type) AS qd_definition_type, max(s1.default_persistence) AS qd_default_persistence, max(s1.default_priority) AS qd_default_priority, max(s1.cluster) AS qd_cluster, max(s1.cluster_namelist) AS qd_cluster_namelist, max(s1.cluster_queue_manager) AS qd_cluster_queue_manager, max(s1.cluster_queue_type) AS qd_cluster_queue_type, max(s1.alter_date_and_time) AS qd_alter_date_and_time, max(s1.cluster_date_and_time) AS qd_cluster_date_and_time, max(s1.qsg_name) AS qd_qsg_name, max(s1.cf_structure_name) AS qd_cf_structure_name, max(s1.qsg_disposition) AS qd_qsg_disposition, max(s1.queue_description_u) AS qd_queue_description_u, max(s1.current_depth) AS qd_current_depth, max(s1.input_opens) AS qd_input_opens, max(s1.output_opens) AS qd_output_opens, max(s1.total_opens) AS qd_total_opens, max(s1.trigger_control) AS qd_trigger_control, max(s1.trigger_type) AS qd_trigger_type, max(s1.trigger_depth) AS qd_trigger_depth, max(s1.trigger_priority) AS qd_trigger_priority, max(s1.get_status) AS qd_get_status, max( s1.high_depth_threshold_percent ) AS qd_high_depth_threshold_percent, max(s1.percent_full) AS qd_percent_full, max(s1.maximum_depth) AS qd_maximum_depth, max(s1.target_or_remote_queue) AS qd_target_or_remote_queue, max(s1.remote_queue_manager) AS qd_remote_queue_manager FROM ( SELECT concat( '20', substring(s0.dwritetime, 2, 10)) AS dwritetime_format0, concat( '20', substring(s0.dwritetime, 2, 9), ( CASE WHEN substring(s0.dwritetime, 11, 1) < '5' THEN '0' ELSE '5' END )) AS dwritetime_format1, s0.* FROM temp_itm_queue_data s0 ) s1 GROUP BY s1.dwritetime_format1, s1.host_name")
      qdDF.registerTempTable("base_itm_qd")

      /** 根据服务器类型选择性关联opm和itm表形成宽表 isDB2 || isWAS || isMQ || OTHER **/

      /** cfgServerType = 'isDB2'  */
      //base_apptrans_ecup base_itm_um base_itm_sys base_opm_db
      val ecupIsDB2 = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.* FROM ( SELECT * FROM base_apptrans_ecup s0 WHERE s0.cfgServerType = 'isDB2' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_opm_db t4 ON t1.d_timestamp0 = t4.odb_opmtime_format0 AND t1.cfgServerIP = t4.opm_db_host_name")
      //base_apptrans_gmp base_itm_um base_itm_sys base_opm_db
      val gmpIsDB2 = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.* FROM ( SELECT * FROM base_apptrans_gmp s0 WHERE s0.cfgServerType = 'isDB2' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_opm_db t4 ON t1.d_timestamp0 = t4.odb_opmtime_format0 AND t1.cfgServerIP = t4.opm_db_host_name")

      /** cfgServerType = 'isWAS'  */
      //base_apptrans_ecup base_itm_um base_itm_sys base_itm_thp base_itm_dcp base_itm_aps
      val ecupIsWAS = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.*, t5.*, t6.* FROM ( SELECT * FROM base_apptrans_ecup s0 WHERE s0.cfgServerType = 'isWAS' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_itm_thp t4 ON t1.d_timestamp1 = t4.thp_dwritetime_format1 AND t1.cfgHostName = t4.thp_node_name LEFT JOIN base_itm_dcp t5 ON t1.d_timestamp1 = t5.dcp_dwritetime_format1 AND t1.cfgHostName = t5.dcp_node_name LEFT JOIN base_itm_aps t6 ON t1.d_timestamp1 = t6.aps_dwritetime_format1 AND t1.cfgHostName = t6.aps_node_name")
      //base_apptrans_gmp base_itm_um base_itm_sys base_itm_thp base_itm_dcp base_itm_aps
      val gmpIsWAS = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.*, t5.*, t6.* FROM ( SELECT * FROM base_apptrans_gmp s0 WHERE s0.cfgServerType = 'isWAS' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_itm_thp t4 ON t1.d_timestamp1 = t4.thp_dwritetime_format1 AND t1.cfgHostName = t4.thp_node_name LEFT JOIN base_itm_dcp t5 ON t1.d_timestamp1 = t5.dcp_dwritetime_format1 AND t1.cfgHostName = t5.dcp_node_name LEFT JOIN base_itm_aps t6 ON t1.d_timestamp1 = t6.aps_dwritetime_format1 AND t1.cfgHostName = t6.aps_node_name")

      /** cfgServerType = 'isMQ'  */
      //base_apptrans_ecup base_itm_um base_itm_sys base_itm_qd
      val ecupIsMQ = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.* FROM ( SELECT * FROM base_apptrans_ecup s0 WHERE s0.cfgServerType = 'isMQ' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_itm_qd t4 ON t1.d_timestamp1 = t4.qd_dwritetime_format1 AND t1.cfgHostName = t4.qd_host_name")
      //base_apptrans_gmp base_itm_um base_itm_sys base_itm_qd
      val gmpIsMQ = hiveContext.sql("SELECT t1.*, t2.*, t3.*, t4.* FROM ( SELECT * FROM base_apptrans_gmp s0 WHERE s0.cfgServerType = 'isMQ' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format LEFT JOIN base_itm_qd t4 ON t1.d_timestamp1 = t4.qd_dwritetime_format1 AND t1.cfgHostName = t4.qd_host_name")

      /** cfgServerType = 'OTHER'  */
      //base_apptrans_ecup base_itm_um base_itm_sys
      val ecupOTHER = hiveContext.sql("SELECT t1.*, t2.*, t3.* FROM ( SELECT * FROM base_apptrans_ecup s0 WHERE s0.cfgServerType = 'OTHER' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format")
      //base_apptrans_gmp base_itm_um base_itm_sys
      val gmpOTHER = hiveContext.sql("SELECT t1.*, t2.*, t3.* FROM ( SELECT * FROM base_apptrans_gmp s0 WHERE s0.cfgServerType = 'OTHER' ) t1 LEFT JOIN base_itm_um t2 ON t1.d_timestamp1 = t2.um_dwritetime_format1 AND t1.cfgHostName = t2.um_host_format LEFT JOIN base_itm_sys t3 ON t1.d_timestamp1 = t3.sys_dwritetime_format1 AND t1.cfgHostName = t3.sys_host_format")

      /** DataFrame 转 rdd 执行map转换为二元组("文件名或表名",ROW.mkString(",")),并作RDD的合并，执行模型预测
        * ecupIsDB2 gmpIsDB2 ecupIsWAS gmpIsWAS ecupIsMQ gmpIsMQ ecupOTHER gmpOTHER
        */
      val ecupRDD = ecupIsDB2.rdd.map(row => ("ECUPISDB2", row.mkString(",")))
        .union(ecupIsWAS.rdd.map(row => ("ECUPISWAS", row.mkString(","))))
        .union(ecupIsMQ.rdd.map(row => ("ECUPISMQ", row.mkString(","))))
        .union(ecupOTHER.rdd.map(row => ("ECUPOTHER", row.mkString(","))))
      val gmpRDD = gmpIsDB2.rdd.map(row => ("GMPISDB2", row.mkString(",")))
        .union(gmpIsWAS.rdd.map(row => ("GMPISWAS", row.mkString(","))))
        .union(gmpIsMQ.rdd.map(row => ("GMPISMQ", row.mkString(","))))
        .union(gmpOTHER.rdd.map(row => ("GMPOTHER", row.mkString(","))))

      /** 此处可合并预测，也可单独预测，看具体情况，目前采用合并预测，使得代码简洁 **/
      /** 执行复合指标的模型预测，调用海涛提供的接口方法，返回预测结果 **/
      val res = ecupRDD.union(gmpRDD).map(values => {
        val resObject = new util.HashMap[String, Object]()
        val res = RealTimeDataProcessingNew.dataProcessing(initial, values._1, values._2, "prediction")
        resObject.put(SysConst.MAP_DATA_GETTIME_KEY, dataGetTime.toString)
        resObject.put(SysConst.MAP_DATA_RESULT_KEY, res)
        resObject
      })
      /** 将预测结果保存数据库 **/
      val t1 = System.currentTimeMillis()
      val list = res.collect().toList
      val t2 = System.currentTimeMillis()
      logger.warn("****  当前stream collect 处理耗时 rdd.collect().toList  ===> " + (t2 - t1) + " ms")
      new PredResultService().dataAcceptAndSaveMysql(list)
      val t3 = System.currentTimeMillis()
      logger.warn("****  当前stream数据保存处理耗时dataAcceptAndSaveMysql  ===> " + (t3 - t2) + " ms")


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
