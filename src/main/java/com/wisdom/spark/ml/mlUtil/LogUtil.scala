package com.wisdom.spark.ml.mlUtil

import org.apache.log4j.{Logger, PropertyConfigurator};

/**
  * Created by tup on 2016/12/5.
  */
object LogUtil {

  val logPropPath = ContextUtil.mlLogPropsPath

  def getLogger(className: Object): Logger = {
    PropertyConfigurator.configure(logPropPath)
    val log = Logger.getLogger(className.getClass)

    return log
  }

  def getModelLogger(): Logger = {
    PropertyConfigurator.configure(logPropPath)
    val log = Logger.getLogger("model")

    return log
  }
}
