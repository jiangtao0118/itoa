package com.wisdom.spark.common

/**
  *
  * 定义程序运行状态码，状态码由3位组成，第一位为标识位，用于区分各模块，二、三位为状态位，从00开始
  *
  * 公共模块状态码标识位为1
  * 实时计算模块状态码标识位为2
  * 数据转换模块状态码标识位为3
  * 算法模块状态码标识位为4
  *
  * @author yfdang
  * @date 2016年11月28日 15:31
  **/
object ExecutorState extends Enumeration {


  /**
    * 公共状态码定义
    */
  val success = Value(100, "成功")
  val fail = Value(101, "失败")
  val data_format_mismatch = Value(102,"数据格式不匹配")


  /**
    * 实时计算模块状态码定义
    */
  val data_reception_delay = Value(200,"数据接收延迟")



  /**
    * 数据转换模块状态码定义
    */

  val missing_data_field = Value(300,"数据字段缺失")
  val ignore_this_host= Value(301,"忽略此主机")
  val data_convert_exception= Value(302,"数据转换异常")
  val other_exception= Value(303,"其它异常")
  val prediction_exception= Value(304,"预测异常")
  val relationAnalysis_exception= Value(305,"关联分析异常")

  /**
    * 算法模块状态码定义
    */
  val data_source_timeout = Value(400,"数据来源超时")
  val predict_timeout = Value(401,"预测超时")
  val program_internal_error = Value(402,"程序内部错误")
  val data_error = Value(403,"数组长度错误")
  val indicator_unsupported = Value(404,"不支持指标")
  val host_unsupported = Value(405,"不支持主机节点")


  def main(args: Array[String]) {

    println("status code :" + ExecutorState.success.id)

    println("status : " + ExecutorState.success)
  }


}

