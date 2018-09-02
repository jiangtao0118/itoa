package com.wisdom.spark.streaming.test

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.util.{HDFSFileUtil, RandomNum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by htgeng on 2017/6/1.
  */
object TestGenData {
  def main(args: Array[String]) {
    val conf = new Configuration()
    //    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val props = ItoaPropertyUtil.getProperties()
    val sdf=new SimpleDateFormat("'1'yyMMddHHmmssSSS")
    val interval =props.getProperty("spark.streaming.interval.second")
    var i=0
    while(true){
      Thread.sleep(interval.toLong*1000)
      val filename=UUID.randomUUID()+".csv"
      val dateStr=sdf.format(new Date)
      val cpu=RandomNum.getRangeNum(1,20)
      val data1="{\"@hostname\":\"ITMRMS16\",\"@filename\":\"UNIXOS_117\",\"@linenum\":\"1\",\"@filepath\":\"/tivrepos/test/UNIXOS_\",\"@message\":\"-28800,"+dateStr+",ASGAAC01:KUX,"+dateStr+",AIX,6.1,12582784,20971392,166160312,0,133,182.248.56.85,4,1,"+cpu+",0,0,0,279,0,0,22,0,0,14654332,6317060,288085,1781829,242,256,0,0,0,0,0,0,-1,-1,-1,-1,-1,0.30,0.29,0.34,480544,12102240,0,30.2,69.8,5,696256,75127,-1,133,-1,0,0,0,0,-1,-1,1110819112639000,-1,-1,-1,1923d03:38:3,8158,0,87,0,0,0,0,0,37,22,24,24,0,0,0,0,0,No DNS Entry,-1,-1,,,,,,,,,,,,,,,,,\"}"
      val data2="{\"@hostname\":\"ITMRMS16\",\"@filename\":\"UNIXOS_117\",\"@linenum\":\"1\",\"@filepath\":\"/tivrepos/test/UNIXOS_\",\"@message\":\"-28800,"+dateStr+",ASGAAC02:KUX,"+dateStr+",AIX,6.1,12582784,20971392,166160312,0,133,182.248.56.85,4,1,"+cpu+",0,0,0,279,0,0,22,0,0,14654332,6317060,288085,1781829,242,256,0,0,0,0,0,0,-1,-1,-1,-1,-1,0.30,0.29,0.34,480544,12102240,0,30.2,69.8,5,696256,75127,-1,133,-1,0,0,0,0,-1,-1,1110819112639000,-1,-1,-1,1923d03:38:3,8158,0,87,0,0,0,0,0,37,22,24,24,0,0,0,0,0,No DNS Entry,-1,-1,,,,,,,,,,,,,,,,,\"}"
      val data=data1+"\n"+data2
      val path="/user/wsd/streaming/"+filename
      HDFSFileUtil.write(fs,path,data,true)
      println(dateStr+"生成Kafka数据")

    }
    fs.close()

  }

}
