package com.wisdom.spark.ml.mlUtil

import com.wisdom.spark.ml.tgtVar.AllPredcictTarget

import scala.collection.mutable.HashMap

/**
  * Created by tup on 2017/1/12.
  */
@ deprecated
object ModelUtil extends Serializable{

  /**
    * (指标_预测周期_节点,AllPredcictTarget对象)
    * 指标_预测周期_节点: 例如IDEL_CPU_5_ASCECUP01
    */
  val modelMap = tgtModelMap()

  private def tgtModelMap(): HashMap[String,AllPredcictTarget] = {

    val tgtModelMap = new HashMap[String,AllPredcictTarget]
    val idis = ContextUtil.BOUND_MAP.keys.toArray
    val intervals = ContextUtil.INTERVAL.split(",")
    val mlRootPath = ContextUtil.mlRootPath

    var node = ""
    var tgt = ""
    var tmpPath = ""
    for(i<-0 until idis.length){
      for(j<-0 until intervals.length){
        tgt = idis(i) + "_" + intervals(j)
        tmpPath = mlRootPath + "/" + tgt
        if(HDFSFileUtil.exists(tmpPath)){
          val nodes = HDFSFileUtil.listFiles(tmpPath)

          for(k<-0 until nodes.length){
            node =  nodes(k)
            tgtModelMap.put(tgt+"_"+node,new AllPredcictTarget(tgt,node))
          }
        }
      }

    }

    tgtModelMap
  }

  def main(args: Array[String]) {

    modelMap.foreach{case(x,y)=>println(x)}
  }

}
