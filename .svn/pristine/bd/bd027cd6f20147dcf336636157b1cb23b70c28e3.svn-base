package com.wisdom.spark.streaming.tools

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable.Map
import scala.util.parsing.json.JSON


/**
  * Created by zhengz on 2016/11/17.
  * JSON操作工具类，包括解析JSON字符串转换为map方法;Map递归嵌套解析;
  */
object JsonUtil {
  /**
    * 解析JSON格式的数据并已Map[String,Any]类型返回
    *
    * @param str
    * @return
    */
  def parseJSON(str: String): Map[String, Any] = {
    var map: Map[String, Any] = Map()
    val json = JSON.parseFull(str)
    json match {
      case Some(m: scala.collection.immutable.Map[String, Any]) => map = map ++ m
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
    //    map = kv2IfStr(map) //递归map对map进行数据初步清洗工作
    return map
  }


  /**
    * 判断Map的值是否嵌套Map，如果不存在嵌套，则进行清洗操作
    *
    * 暂未考虑空指针异常
    *
    * @param map
    * @return
    */
  def kv2IfStr(map: Map[String, Any]): Map[String, Any] = {
    map.keys.foreach { m_k =>
      map(m_k) match {
        case m_v1: String => map += (m_k -> dataClean(m_v1))
        case m_v2: Map[String, Any] => map(m_k) -> kv2IfStr(m_v2)
      }
    }
    return map
  }

  /**
    * 数据清洗规则(初步清洗，例如去空格)
    *
    * 暂未考虑空指针异常
    *
    * @param str
    * @return
    */
  def dataClean(str: String): String = {
    val sArray = str.split(",", -1)
    val stringBuilder = new StringBuilder("");
    for (i <- 0 to (sArray.length - 1)) {
      stringBuilder.append(sArray(i).trim() + ",")
    }
    return stringBuilder.deleteCharAt(stringBuilder.length - 1).toString()
  }

  /**
    * 将JAVA对象转换为JSON字符串
    *
    * @param obj
    * @return
    */
  def write2JSON(obj: Object): String = {
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsString(obj)
  }

}
