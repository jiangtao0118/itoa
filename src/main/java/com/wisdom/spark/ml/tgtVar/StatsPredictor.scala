package com.wisdom.spark.ml.tgtVar

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.wisdom.spark.common.util.{ConnPoolUtil2, SparkContextUtil}
import com.wisdom.spark.etl.bean.DBDataBean
import com.wisdom.spark.ml.mlUtil.{ContextUtil, HDFSFileUtil, MLPathUtil}
import com.wisdom.spark.ml.model.dynamicModel.{Stat, StatFromHive, StatFromHive2}
import com.wisdom.spark.streaming.tools.{AllDataFrames, DateFormatUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by tup on 2017/4/25.
  */
class StatsPredictor (tgt: String,
                      hostnode: String) extends Serializable with PredictTrait{

  val tgt_ind = tgt.substring(0, tgt.lastIndexOf("_"))
  val tgt_int = tgt.substring(tgt.lastIndexOf("_") + 1)
  val indexName=tgt.substring(0,tgt.length-2)
  val intervals = ContextUtil.INTERVAL.split(",")
  val winstatsFreq5min=ContextUtil.winstatsFreq5min
  val mlWeeksCount=ContextUtil.mlTrainCount
  val window=ContextUtil.mlWindow
//  val indexInfoMap=ContextUtil.indexInfoMap
//  val hiveCtx=AllDataFrames.hiveCtx
//  val df=getDataFrame(hostnode,indexName)

  val delta: Int = {
    var tmp=0
    if(tgt_int==intervals(0)){
      tmp=5
    }
    else if(tgt_int==intervals(1)){
      tmp=15
    }
    else if(tgt_int==intervals(2)){
      tmp=30
    }
    else if(tgt_int==intervals(3)){
      tmp=60
    }
    tmp
  }

  val modelPath = MLPathUtil.statsModelPath(tgt, hostnode)
  val model = getModelMap()

  def predict(data: Array[Double]): Array[Double] = {
    val out = new Array[Double](3)

    //传人数组的前五个字段是时间：Year,Month,Day,Hour,Minute
    val yr = data(0).toInt
    val mon = data(1).toInt
    val day = data(2).toInt
    val hour = data(3).toInt
    val min = data(4).toInt

    var avg = 0.0
    var std = 0.0

    val key=getkey(yr,mon,day,hour,min)


    val value = model.get(key)

    if(value.nonEmpty){
      avg=value.get._1
      std= value.get._2
    }
    else{
      val avgArr = model.values.toArray.map(x=>x._1)
      val stdArr = model.values.toArray.map(x=>x._2)
      avg=avgArr.sum/avgArr.length
      std=stdArr.sum/stdArr.length
    }

    out(0)=avg
    out(1)=avg - 2 * std
    if(out(1)<0){
      out(1)=avg - std
    }
    if(out(1)<0){
      out(1)=avg
    }
    out(2)=avg + 3 * std

    out
  }

  private def getkey(year: Int,month: Int,day: Int,hour: Int,min: Int): String={
    val ins = Calendar.getInstance()

    ins.set(Calendar.HOUR_OF_DAY,hour)

    var freqMin=min
    if(winstatsFreq5min.contains(tgt_ind)){
      freqMin=min/5*5+1
    }
    ins.set(Calendar.MINUTE,freqMin)
//    ins.add(Calendar.MINUTE,delta)

    val df=new SimpleDateFormat("HH:mm")
    df.format(ins.getTime)
    val out = df.format(ins.getTime)

    ins.set(Calendar.YEAR,year)
    ins.set(Calendar.MONTH,month-1)
    ins.set(Calendar.DAY_OF_MONTH,day-1)
    val dayOfWeek=ins.get(Calendar.DAY_OF_WEEK)%7


    dayOfWeek+"_"+out
  }

  private def getModelMap(): mutable.HashMap[String,(Double,Double)] = {
    //时间、均值、标准差

    val avg_std=ContextUtil.sc.textFile(modelPath).map(x=>x.split(",")).map{x=>
      try{
        (x(0),x(1).toDouble,x(2).toDouble)
      }catch{case e:Exception=>
          println("-----"+modelPath+"---转doouble错误-----:"+x.toString)
          println(x(1),x(2))
        (x(0),0.0,0.0)
      }
      }.collect()
    val avg_std_map=new mutable.HashMap[String,(Double,Double)]

    for(i<-0 until avg_std.length){
      avg_std_map.put(avg_std(i)._1,(avg_std(i)._2,avg_std(i)._3))
    }

    avg_std_map
  }


  private def getModelMap2(count:Int,window:Int): mutable.HashMap[String,(Double,Double)] = {
    //时间、均值、标准差
    val conn=ConnPoolUtil2.getConn()
    val hostName=hostnode

    val indexName=tgt.substring(0,tgt.length-2)
    println("---------------------hostName:"+hostName)
    println("---------------------tgt:"+indexName)

    val modelMap=Stat.getModel(conn,hostName,indexName,count,window)
    if (conn != null) {
      //释放数据库连接
      ConnPoolUtil2.releaseCon(conn)
    }

    println("modelMap Length:"+modelMap.keys.toList.length)
    modelMap
  }

  private def getModelMap3(df:DataFrame,window:Int): mutable.HashMap[String,(Double,Double)] = {
    var modelMap=new mutable.HashMap[String,(Double,Double)]
    //时间、均值、标准差
    if(df!=null){
      modelMap=StatFromHive2.getModel(df,indexName,window)
    }
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!modelMap Length:"+modelMap.keys.toList.length)
    modelMap
  }


//  def getDataFrame(hostname:String,indexName:String):DataFrame={
//    var df:DataFrame=null
//    var sqlTemp=""
//    if(indexInfoMap.contains(indexName)) {
//      sqlTemp = indexInfoMap.get(indexName).get
//      sqlTemp = sqlTemp.replace("HOSTNAME", hostname)
//      sqlTemp = sqlTemp.replace("INDEXNAME", indexName)
//      df = hiveCtx.sql(sqlTemp)
//    }
//    df
//  }

//  private def getResult():DataFrame={
//    var sqlTemp=""
//    var result:DataFrame=null
//    if(indexInfoMap.contains(indexName)) {
//      sqlTemp = indexInfoMap.get(indexName).get
//      sqlTemp = sqlTemp.replace("HOSTNAME", hostnode)
//      sqlTemp = sqlTemp.replace("INDEXNAME", indexName)
//      sqlTemp = sqlTemp.replace("COUNT", mlWeeksCount.toString)
//
//      result = hiveCtx.sql(sqlTemp)
//    }
//    result
//  }








}

object StatsPredictor{
  def main(args: Array[String]) {
    val stats=new StatsPredictor("ECUPIN_AVG_TRANS_TIME_0","ASCECUP01")
//    println(stats.getkey(1,58))
//    println(stats.getkey(23,58))
  }
}