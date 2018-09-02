package com.wisdom.spark.ml.mlUtil

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by wisdom on 2016/11/23.
  */
object PersitObjectUtil {

  def writeObjectToFile(obj:Object,fileUri: String): Unit = {

    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(fileUri)

    val outStream = fs.create(dst,true)
    val ostream = new ObjectOutputStream(outStream)
    ostream.writeObject(obj)
    outStream.flush()
    ostream.flush()
    outStream.close()
    ostream.close()
  }

  def readObjectFromFile(fileUri: String): Object = {

    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(fileUri)
    val hdfsInStream = fs.open(dst)
    val objIn = new ObjectInputStream(hdfsInStream)
    val obj = objIn.readObject()
    hdfsInStream.close()
    objIn.close()

    obj
  }
}
