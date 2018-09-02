package com.wisdom.spark.etl.util

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wisdom on 2016/11/14.
  */
object HDFSFileUtil {


  def main(args: Array[String]): Unit ={
//    HDFSFileUtil.moveFile("C:\\Users\\wisdom\\Desktop\\word2vecModel_tmp\\tmp\\","C:\\Users\\wisdom\\Desktop\\word2vecModel")
//      println(FileUtil.deleteFile("C:\\Users\\wisdom\\Desktop\\tmp2"))
//    println(HDFSFileUtil.moveFile("C:\\Users\\wisdom\\Desktop\\tmp1","C:\\Users\\wisdom\\Desktop\\tmp2"))
  }

  def saveFile()={

  }

  def moveFile(fs:FileSystem,fromPath: String, toPath: String): Boolean = {
    val src = new Path(fromPath)
    val dst = new Path(toPath)
//    deleteFile(toPath) && fs.rename(src,dst)
    fs.rename(src,dst)

  }

  def deleteFile(fs:FileSystem,path:String):Boolean = {
    val dst = new Path(path)
    var rst = true
    if(fs.exists(dst)){
      rst = fs.delete(dst,true)
    }

    rst
  }

  def exists(fs:FileSystem,path:String):Boolean = {
    val dst = new Path(path)
    fs.exists(dst)
  }

  def write(fs:FileSystem,path:String,data:String,overwrite: Boolean): Unit = {
    val dst = new Path(path)

    val outStream = fs.create(dst,overwrite)
    outStream.writeBytes(data)
    outStream.flush()
    outStream.close()
  }

  def write(fs:FileSystem,path:String,overwrite: Boolean): FSDataOutputStream = {
    val dst = new Path(path)

    fs.create(dst,overwrite)

  }

  def appendFile(fs:FileSystem,path:String,data:String): Unit = {
    val dst = new Path(path)

    val outStream = fs.append(dst)
    outStream.writeBytes(data)
    outStream.flush()
    outStream.close()
  }

  def appendFile(fs:FileSystem,path:String): FSDataOutputStream = {
    val dst = new Path(path)

    fs.append(dst)
  }

  def closeSteam(outStream: FSDataOutputStream): Unit = {
    outStream.flush()
    outStream.close()
  }

  def listFiles(fs:FileSystem,path:Path): Array[Path] = {
    val fileArr = new ArrayBuffer[Path]()
    val dst = path
    val files = fs.listStatusIterator(dst)
    while(files.hasNext){
      fileArr.append(files.next().getPath)
    }
    fileArr.toArray
  }

  def read(fs:FileSystem,path:String,encoding: String): BufferedReader = {
    val dst = new Path(path)

    new BufferedReader(new InputStreamReader(fs.open(dst),encoding))
  }

}
