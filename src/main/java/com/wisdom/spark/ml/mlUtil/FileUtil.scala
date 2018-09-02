package com.wisdom.spark.ml.mlUtil

/**
  * Created by wisdom on 2016/6/7.
  */
import java.io.File

object FileUtil {

  def main(args: Array[String]): Unit ={
//    println(FileUtil.deleteFile("E:\\packages\\tmp"))
    println(FileUtil.moveFile("C:\\Users\\wisdom\\Desktop\\tmp1","C:\\Users\\wisdom\\Desktop\\tmp2"))
  }

  def moveFile(fromPath: String, toPath: String): Boolean = {
    val tf = new File(toPath)
    val ff = new File(fromPath)
    this.deleteFile(toPath) && ff.renameTo(tf)
  }

  /*
    *  只删除本地文件。既然上传了jar文件到spark服务器执行，为什么没有删除服务器上的文件呢？
    *  不会删除远程spark服务器上的文件；通过spark-submit在远程服务器执行会删除服务器上的文件。
    * */
  def deleteFile(path:String):Boolean={
    val sPath=path

    val dirFile = new File(sPath)
    //如果dir对应的文件不存在，或者不是一个目录，则退出
    if (!dirFile.exists()) {
      return true
    }

    if (dirFile.isDirectory()) {
      deleteDirFile(path)
    } else {
      deleteSingleFile(path)
    }

  }

  /**
    * 删除单个文件
    *
    * @param   sPath    被删除文件的文件名
    * @return 单个文件删除成功返回true，否则返回false
    */
  private def deleteSingleFile(sPath:String):Boolean={
    var flag = false
    val file = new File(sPath)
    // 路径为文件且不为空则进行删除
    if (file.isFile() && file.exists()) {
      file.delete()
      flag = true
    }
    return flag
  }

  /**
    * 删除目录（文件夹）以及目录下的文件
    *
    * @param   path 被删除目录的文件路径
    * @return  目录删除成功返回true，否则返回false
    */
  private def deleteDirFile(path:String):Boolean={
    var sPath=path
    //如果sPath不以文件分隔符结尾，自动添加文件分隔符
    if (!sPath.endsWith(File.separator)) {
      sPath = sPath + File.separator
    }
    val dirFile = new File(sPath)
    //如果dir对应的文件不存在，或者不是一个目录，则退出
    if (!dirFile.exists() || !dirFile.isDirectory()) {
      return false
    }
    var flag = true
    //删除文件夹下的所有文件(包括子目录)
    var files = dirFile.listFiles()
    for (i <- 0 until files.length) {
      //删除子文件
      if (files(i).isFile()) {
        flag = deleteSingleFile(files(i).getAbsolutePath())
        if (!flag) return false
      } //删除子目录
      else {
        flag = deleteDirFile(files(i).getAbsolutePath())
        if (!flag) return false
      }
    }
    if (!flag) return false
    //删除当前目录
    if (dirFile.delete()) {
      true
    } else {
      false
    }
  }

}
