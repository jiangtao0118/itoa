package com.wisdom.spark.streaming.tools

import java.io.{File, FileOutputStream, OutputStreamWriter}

import com.wisdom.spark.common.util.ItoaPropertyUtil
import com.wisdom.spark.etl.DataProcessing.RealTimeDataProcessingNew
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.streaming.thread.Thread4ITMDataCollect2

/**
  * Created by zhengz on 2016/11/24.
  * IO操作工具类，目前仅包括字符串写入指定路径的方法
  */
object FileUtils {
  val props = ItoaPropertyUtil.getProperties()
  val encoding = props.getProperty("public.sys.encoding")

  /**
    * 文件写入方法
    *
    * @param path 文件所在目录
    * @param name 文件名
    * @param text 文件内容
    * @param is_N 是否追加或重写
    */
  def write2Path(path: String, name: String, text: String, is_N: Boolean): Unit = {
    val dir: File = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file: File = new File(path + System.getProperty("file.separator") + name)
    if (!file.exists()) {
      file.createNewFile()
    }
    val stream = new OutputStreamWriter(new FileOutputStream(file, true), encoding)
    if (is_N) {
      stream.write(text + "\n")
    } else {
      stream.write(text)
    }
    stream.flush()
    stream.close()
  }

}
