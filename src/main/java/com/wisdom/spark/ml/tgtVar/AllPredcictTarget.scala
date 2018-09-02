package com.wisdom.spark.ml.tgtVar

import java.text.DecimalFormat
import java.util
import java.util.Calendar

import com.wisdom.spark.common.ExecutorState
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj.ModelRecord
import org.apache.log4j.Logger

/**
  * Created by tup on 2016/11/17.
  * tgt取值是预测指标标识：如 IDLE_CPU_5
  * hostnode是主机节点
  * data是待预测点，传人数组的前五个字段是时间：Year,Month,Day,Hour,Minute
  */
class AllPredcictTarget(modelObj: ModelRecord) extends Serializable {

  val modelObjInst=modelObj
  def this(preTgt: String,node: String)=this{

    var tgt_ind1: String=null
    var tgt_int1: String=null

    if (preTgt != null && node != null){
      tgt_ind1 = preTgt.substring(0, preTgt.lastIndexOf("_"))
      tgt_int1 = preTgt.substring(preTgt.lastIndexOf("_") + 1)
    }
    val defaultppn=ModelRecord(
      tgt_ind1,
      node,
      tgt_int1,
      ContextUtil.model_ppn,
      "01",
      "",
      "",
      "")

    defaultppn
  }

  var preTgt: String=null
  var node: String=null
  var tgt_ind: String=null
  var tgt_int: String=null

  val BOUND_MAP = ContextUtil.BOUND_MAP
  val INTERVAL = ContextUtil.INTERVAL

  val winstatstgt=ContextUtil.model_winstats
  val ppn=ContextUtil.model_ppn

  var gmmModel: PredictTrait = null
  if (modelObj != null) {

    preTgt=modelObj.tgtvar+"_" + modelObj.period
    node=modelObj.hostnode
    tgt_ind = modelObj.tgtvar
    tgt_int = modelObj.period

    if(modelObj.model_typ==winstatstgt){
      gmmModel = new StatsPredictor(preTgt, node)
    }
    else if (modelObj.model_typ==ppn){
      gmmModel = new GMMPredictorWithCorr(preTgt, node)
    }

  }


  /**
    *
    * @param tgt      取值是预测指标标识：如 IDLE_CPU_5
    * @param hostnode 主机节点
    * @param data     待预测点，传人数组的前五个字段是时间：Year,Month,Day,Hour,Minute
    * @return 预测值 下界 上界
    */
  def predict(tgt: String, hostnode: String, data: Array[Double]): Array[Double] = {
    @transient
    val logger = Logger.getLogger(this.getClass)

    logger.warn("指标: " + preTgt + ", 节点: " + node + ", 接收到实时预测数据: " + data.mkString(","))

    var status = ExecutorState.success
    val delayFromSrc = 1 //分钟 允许最大延迟时间
    var pred: Array[Double] = new Array[Double](3)

    //传人数组的前五个字段是时间：Year,Month,Day,Hour,Minute
    val yr = data(0).toInt
    val mon = data(1).toInt
    val day = data(2).toInt
    val hour = data(3).toInt
    val min = data(4).toInt

    //基准时间
    val tmp1 = Calendar.getInstance()
    //数据时间
    val tmp2 = Calendar.getInstance()
    tmp2.set(Calendar.DAY_OF_MONTH, day)
    tmp2.set(Calendar.HOUR_OF_DAY, hour)
    tmp2.set(Calendar.MINUTE, min)
    tmp2.add(Calendar.MINUTE, delayFromSrc)
    //如果超时就不预测
    //    if(tmp2.compareTo(tmp1)>0){
    //      status = ExecutorState.data_source_timeout
    //    }
    if (false) {
      status = ExecutorState.data_source_timeout
      logger.error("*************************" + ExecutorState.data_source_timeout + "*************************")
    }
    else if (INTERVAL.split(",").contains(tgt_int) && BOUND_MAP.contains(tgt_ind)) {

      var rst : Array[Double] = null
      if(modelObj.model_typ==winstatstgt){
        rst = statsPredicts(data)
      }
      else if(modelObj.model_typ==ppn){
        rst = gmmPredicts(data)
      }

      //          后处理：保留2位小数，处理上下界等
      pred = afterProcess(rst, tgt_ind)

      logger.warn("指标: " + tgt + ", 节点: " + hostnode + ", 预测结果: " + rst.mkString(","))
      logger.warn("指标: " + tgt + ", 节点: " + hostnode + ", 预测值处理后结果: " + pred.mkString(","))

    }
    else {
      status = ExecutorState.indicator_unsupported
      logger.error("*************************" + ExecutorState.indicator_unsupported + "*************************")
      throw new RuntimeException("*************************不支持此指标*************************")
    }

    val endTime = System.currentTimeMillis().toString

    pred

  }

  private def gmmPredicts(data: Array[Double]):Array[Double]={
    @transient
    val logger = Logger.getLogger(this.getClass)

    val m = gmmModel.asInstanceOf[GMMPredictorWithCorr]

    if (m.featureCols.length != data.length + 1) {
//      status = ExecutorState.data_error
      logger.error("*************************" + ExecutorState.data_error + "模型数组需要长度:"+m.featureCols.length+" 实际长度:"+data.length + 1+"*************************")
      logger.error("*************************表头信息:" + m.featureCols.mkString(",")+"*************************")
      throw new RuntimeException("*************************数组长度错误*************************")
    }
    else if (m.corrDataCols.length == 0) {
//      status = ExecutorState.host_unsupported
      logger.error("*************************" + ExecutorState.host_unsupported + "*************************")
      throw new RuntimeException("*************************不支持此主机*************************")
    }

    m.predict(data)
  }

  private def statsPredicts(data: Array[Double]):Array[Double]={
    val m = gmmModel.asInstanceOf[StatsPredictor]
    m.predict(data)
  }

  private def afterProcess(arr: Array[Double], tgt_ind: String): Array[Double] = {

    val low = BOUND_MAP.get(tgt_ind).get(0)
    val upp = BOUND_MAP.get(tgt_ind).get(1)

    val format = "0.00"
    val df = new DecimalFormat(format)

    val bounded = arr.map(x => math.min(math.max(low, x), upp))

    //预测、下界、上界
    var out = bounded
    if ("IDLE_CPU,AVAIL_REAL_MEM_PCT,AVAIL_SWAP_SPACE_PCT".split(",").contains(tgt_ind)) {
      out = bounded.map(x => 100 - x)

      val l = out(1)
      val u = out(2)
      out(1) = u
      out(2) = l
    }

    out.map(x => df.format(x).toDouble)
  }

  /**
    *
    * @param tgt      取值是预测指标标识：如 IDLE_CPU_5
    * @param hostnode 主机节点
    * @param data     待预测点，传人数组的前五个字段是时间：Year,Month,Day,Hour,Minute
    * @return 敏感度分析结果，HashMap，Key是字段名称，Value是对应占比
    */
  def sensitivityAnalysis(tgt: String, hostnode: String, data: Array[Double]): util.HashMap[String, Double] = {

    val sen_rate = 0.03
    val cpuGMM = gmmModel.asInstanceOf[GMMPredictorWithCorr]
    val senOut = cpuGMM.sensitivityAnalysis(data, sen_rate)

    //敏感度
    var out = new Array[Double](senOut.length / 2)
    for (i <- 0 until out.length) {
      out(i) = senOut(2 * i + 1) - senOut(2 * i)
    }

    val smooth = 0.1
    val outsum = out.map(x => math.abs(x)).sum + out.length * smooth
    //敏感度占比 加入平滑项，防止NaN
    for (i <- 0 until out.length) {
      out(i) = (math.abs(out(i)) + smooth) / outsum
    }

    val format = "0.0000"
    val df = new DecimalFormat(format)
    val formatOut = out.map(x => df.format(x).toDouble)

    //组合字段名称和敏感度占比
    val outMap = new util.HashMap[String, Double]

    for (i <- 0 until out.length) {
      outMap.put(cpuGMM.corrDataCols(i), formatOut(i))
    }

    outMap
  }

}

object AllPredcictTarget {
  def main(args: Array[String]) {
    val tgt = "ECUPIN_AVG_TRANS_TIME"
    val node = "ASCECUP01"
    val period="0"
    val data = "2017,11,6,11,19,363,4,1,95,0,3,0,1645,0,5,180,0,0,40326936,593420,4252629,2449,1543,0,0,0,0,3.02,3.05,3.23,95,19.9,80.1,5,1686552,270188,363,0,0,0,0,19,722,380,0,5,2,0,154,180,163,184,86,95,93,109,0,363,4,1,94,0,3,0,438,0,0,151,0,0,40327044,591750,4147721,449,461,0,0,0,0,2.77,3.05,3.28,93,19.9,80.1,5,1799479,282114,363,0,0,0,0,0,603,372,0,0,0,0,129,151,181,188,42,93,95,111,0,363,4,1,95,0,3,0,437,0,0,159,0,0,40327456,588857,4221655,636,610,0,0,0,0,3.02,3.39,3.46,92,19.9,80.1,5,1744093,274631,363,0,0,0,0,0,636,368,0,0,0,0,128,159,183,191,95,92,122,119,0,363,5,2,93,0,3,0,328,0,0,232,0,0,40327700,608334,4489487,425,412,0,0,0,0,4.48,3.91,3.59,99,19.9,80.1,7,1946482,319555,363,0,0,0,0,0,929,396,0,0,0,0,171,232,180,196,72,99,119,121,0,0,0,0,1,0,0,0,1207,0,5,29,0,0,-108,1670,104908,2000,1082,0,0,0,0,0.25,0,-0.049999952,2,0,0,0,-112927,-11926,0,0,0,0,0,19,119,8,0,5,2,0,25,29,-18,-4,44,2,-2,-2,0,0,0,0,-1,0,0,0,1,0,0,-8,0,0,-412,2893,-73934,-187,-149,0,0,0,0,-0.25,-0.34000015,-0.18000007,1,0,0,0,55386,7483,0,0,0,0,0,0,-33,4,0,0,0,0,1,-8,-2,-3,-53,1,-27,-8,0,0,-1,-1,2,0,0,0,109,0,0,-73,0,0,-244,-19477,-267832,211,198,0,0,0,0,-1.46,-0.52,-0.12999988,-7,0,0,-2,-202389,-44924,0,0,0,0,0,0,-293,-28,0,0,0,0,-43,-73,3,-5,23,-7,3,-2,0,97"
      .split(",").map(x => x.toDouble)

    val datarecord = ModelRecord(
      tgt,
      node,
      period,
      ContextUtil.model_ppn,
      "01",
      "",
      "",
      "")
//    val pre = new AllPredcictTarget(datarecord)
//    val pre = new AllPredcictTarget(null, null)
    val pre = new AllPredcictTarget("IDLE_CPU_0", "ASCECUP01")
    println(pre.predict(tgt,node,data).mkString(","))
  }
}