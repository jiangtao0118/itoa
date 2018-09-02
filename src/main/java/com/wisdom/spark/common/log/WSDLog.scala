package com.wisdom.spark.common.log


import org.apache.log4j.{Logger, PropertyConfigurator};


/**
  *
  * 日志级别说明：
  * fatal - 严重的，造成服务中断的错误；
  * error - 运行期错误；
  * warn - 警告信息，如程序调用了一个即将作废的接口，接口的不当使用，运行状态不是期望的但仍可继续处理等；
  * info - 有意义的事件信息，如程序启动，关闭事件，收到请求事件等；
  * debug - 调试信息，可记录详细的业务处理到哪一步了，以及当前的变量状态；
  * trace - 更详细的跟踪信息；
  *
  * 通过修改conf目录下的rootLogger可以设置日志级别
  *
  * @author yfdang
  * @date 2016年11月30日 9:13
  **/
object WSDLog extends Serializable {

  /**
    *
    * @param className
    * @return
    */
  def getLogger(className: Object): Logger = {
    var fileName = "/home/wsd/zhengz/conf/conf/log4j.properties"
    if (System.getProperty("os.name").contains("Windows")) {
      fileName = "./conf/log4j.properties"
    } else {
      fileName = "/home/wsd/zhengz/conf/log4j.properties"
    }
    //    PropertyConfigurator.configure(fileName);
    val log = Logger.getLogger(className.getClass)
    return log
  }


  def main(args: Array[String]) {

    val log = WSDLog.getLogger(WSDLog)

    log.debug("debug....")
    log.info("info....")
    log.warn("warn....")
    log.error("error")

  }
}
