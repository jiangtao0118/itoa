package com.wisdom.spark.etl.util

import scala.util.Random

/**
  * Created by htgeng on 2017/3/29.
  */
object RandomNum {
  def main(args: Array[String]) {

    println(getNums(3))
//    for(i<-0 until 100){
//      println(getRangeString(0,60))
//    }

  }

  /**
    *  获取指定长度的数字，如5位数字
    * @param length 数字位数
    * @return
    */
  def getNums(length:Int):String={
    val sb=new StringBuilder
    for(i<- 0 until length){
      sb.append(Random.nextInt(10))
    }
    sb.toString()
  }

  /**
    * 获取指定范围的数字，如1-9，将产生00-09式样
    * @param start 开始值
    * @param end 结束值
    * @return
    */
  def getRangeString(start:Int,end:Int):String={
    val num=Random.nextInt(end)
    var str="01"
    if(num>=start&& num<end){
      if(num<10){
        str="0"+num
      }else{
        str=""+num
      }
    }else{
      str=getRangeString(start,end)
    }
    str
  }

  /**
    * 获取指定范围的数字，如1-100，将产生1 66 88这样的数字
    * @param start 开始值
    * @param end 结束值
    * @return
    */
  def getRangeNum(start:Int,end:Int):Int={
    var num=Random.nextInt(end)
    if(num>=start&& num<end){

    }else{
      num=getRangeNum(start,end)
    }
    num
  }

  def getSortString(start:Int,step:Int):String={
    var str="01"
    val num=start+step
    if(num<10){
      str="0"+num
    }else{
      str=""+num
    }
    str
  }


}
