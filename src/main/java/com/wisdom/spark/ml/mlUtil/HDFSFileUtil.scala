package com.wisdom.spark.ml.mlUtil

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wisdom on 2016/11/14.
  */
object HDFSFileUtil {

  def main(args: Array[String]): Unit ={
    HDFSFileUtil.moveFile("C:\\Users\\wisdom\\Desktop\\word2vecModel_tmp\\tmp\\","C:\\Users\\wisdom\\Desktop\\word2vecModel")
//      println(FileUtil.deleteFile("C:\\Users\\wisdom\\Desktop\\tmp2"))
//    println(HDFSFileUtil.moveFile("C:\\Users\\wisdom\\Desktop\\tmp1","C:\\Users\\wisdom\\Desktop\\tmp2"))
  }

  def moveFile(fromPath: String, toPath: String): Boolean = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val src = new Path(fromPath)
    val dst = new Path(toPath)
//    deleteFile(toPath) && fs.rename(src,dst)
    fs.rename(src,dst)

  }

  def deleteFile(path:String):Boolean = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)
    var rst = true
    if(fs.exists(dst)){
      rst = fs.delete(dst,true)
    }

    rst
  }

  def exists(path:String):Boolean = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)
    fs.exists(dst)
  }

  def write(path:String,data:String,overwrite: Boolean): Unit = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)

    val outStream = fs.create(dst,overwrite)
    outStream.writeBytes(data)
    outStream.flush()
    outStream.close()
  }

  def write(path:String,overwrite: Boolean): FSDataOutputStream = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)

    fs.create(dst,overwrite)

  }

  def appendFile(path:String,data:String): Unit = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)

    val outStream = fs.append(dst)
    outStream.writeBytes(data)
    outStream.flush()
    outStream.close()
  }

  def appendFile(path:String): FSDataOutputStream = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)

    fs.append(dst)
  }

  def closeSteam(outStream: FSDataOutputStream): Unit = {
    outStream.flush()
    outStream.close()
  }

  def listFiles(path:String): Array[String] = {
    val fileArr = new ArrayBuffer[String]()
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path + "/")
    val files = fs.listStatusIterator(dst)
    while(files.hasNext){
      fileArr.append(files.next().getPath.getName)
    }
    fileArr.toArray
  }

  def read(path:String,encoding: String): BufferedReader = {
    val fs = FileSystem.get(ContextUtil.hdfsConf)
    val dst = new Path(path)

    new BufferedReader(new InputStreamReader(fs.open(dst),encoding))
  }

  def closeReader(reader: BufferedReader) = {
    reader.close()
  }

}
