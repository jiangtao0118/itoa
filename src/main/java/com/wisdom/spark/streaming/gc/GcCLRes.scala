package com.wisdom.spark.streaming.gc

/**
  * Created by wisdom on 2017/3/10.
  *
  * 用于封装 GC异常日志识别预测结果 的类
  */
case class GcCLRes (
                     exclusiveStart_timestamp:String,//时间 eg:2017-02-01T10:16:51.624
                     hostname:String,//主机 eg: ASCMEBS01,ASCMEBS02,ASCMOBS01,ASCECUP01
                     gc_type:String,//类型 eg:scavenge,global
                     abnormal:Boolean,//结果 eg:true 异常 false 正常
                     gcEnd_durationms:String,//持续时间 eg:22.123
                     gcStart_mem_percent:String,// gc开始内存使用率
                     gcEnd_mem_percent:String,// gc结束内存使用率
                     //由exclusiveStart_timestamp,gcStart_timestamp，gcEnd_timestamp,exclusiveEnd_timestamp构成的识别唯一一条日志的JSON字符串
                     unique_gcInfo:String
                   )

