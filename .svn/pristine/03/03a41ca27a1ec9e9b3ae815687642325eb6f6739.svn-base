package com.wisdom.spark.etl.util

/**
  * Created by htgeng on 2017/1/6.
  */
object DataFormat {

  def main(args: Array[String]) {
    val opm="182.242.10.244,MAINDB,2015-09-22 02:00:00.0,ACTIVE,99,1,287624670,4720964130,819840,0,0,0,0,78.0,0,81,0.782051282051282,2,55805,5214.0,0,100.0,0,0,0,0,0,0,0,0,0"
    val itm=""

  }

  def dataFormat(initUtil:InitUtil, str:String):String={
    var formatString=""
    try{
      if(initUtil.currentTableName.equalsIgnoreCase("OPMDB")){
        val arr=str.split(",")
        formatString="-28880"+","+DateUtil.getBOCTimeStamp2(arr(2))+","+getOPMDBHostName(initUtil,arr(0))+","+str
        formatString
      }else{
        str
      }
    }catch{
      case e:Exception=>println("wrong data format")
        "NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR,NONE_EXIST:ERROR"
    }

  }

  def getOPMDBHostName(initUtil:InitUtil,str:String):String={
    var hostname="NONE_EXIST:ERROR"
    if(initUtil.OPMDBHostsMap.containsKey(str)){
      hostname=initUtil.OPMDBHostsMap.get(str)+":"+str
    }
    hostname
  }
}
