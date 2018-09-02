package com.wisdom.spark.streaming.tools

import com.wisdom.java.common.LoginUtil
import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2
import org.apache.hadoop.conf.Configuration

/**
  * Created by zhengz on 2016/11/29.
  * 华为hadoop平台产品认证工具类
  */
object KerbInit {
  /**
    * 华为产品Kerb认证初始化过程，开启连接端口
    */
  def init(): Unit = {
    val props = ItoaPropertyUtil.getProperties()
    /**
      * 华为krb认证信息配置
      */
    val userPrincipal = props.getProperty("public.hadoop.userPrincipal")
    val userKeytabPath = props.getProperty("public.hadoop.userKeytabPath")
    val krb5ConfPath = props.getProperty("public.hadoop.krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf) //登录认证，打开端口 测试环境关闭
  }
}
