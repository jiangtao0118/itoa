package com.wisdom.spark.common.util

import com.wisdom.java.common.LoginUtil
import com.wisdom.spark.ml.mlUtil.PropertyUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengz on 2017/1/11.
  * 提供获取SparkConext的工具类，统一管理sc的获取
  */
object SparkContextUtil extends Serializable {

  val props = ItoaPropertyUtil.getProperties()

  val userPrincipal = props.getProperty("public.hadoop.userPrincipal")
  val userKeytabPath = props.getProperty("public.hadoop.userKeytabPath")
  val krb5ConfPath = props.getProperty("public.hadoop.krb5ConfPath")
  val hadoopConf: Configuration = new Configuration()

  val appName = props.getProperty("public.app.name")
  val master = props.getProperty("sparkMaster")

  private var sc: SparkContext = null

  def login(): Unit = {
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
  }

  val sparkConf = new SparkConf().setAppName(appName)

  if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    //        System.setProperty("hadoop.home.dir", "F:\\ref\\Spark\\hadoop-common-2.2.0-bin-master")
    sparkConf.setMaster(props.getProperty("sparkMaster"))
  }
  else {
    login()
//    sparkConf.setMaster(master)
  }

  /**
    * 获取sc
    *
    * @return
    */
  def getInstance(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(sparkConf)
    }
    sc
  }

}
