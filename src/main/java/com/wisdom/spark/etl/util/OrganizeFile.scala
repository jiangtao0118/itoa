package com.wisdom.spark.etl.util

import java.io.File


/**
  * Created by htgeng on 2017/2/14.
  */
object OrganizeFile {
  def main(args: Array[String]): Unit = {
    val path=args(0)
    val tableNameFolders=new File(path)
    val hostnameFolders=tableNameFolders.listFiles()
    for(hostnameFolder<-hostnameFolders){
      if(hostnameFolder.isDirectory){
        val files=hostnameFolder.listFiles()
        for(file<-files){
          if(file.isFile&&file.getName.endsWith("csv")){
            val targetName=file.getName.split("\\.")(0)
            val fileDest=new File(path+hostnameFolder.getName+"/"+targetName+"/"+hostnameFolder.getName+".csv")
            val parentFile=fileDest.getParentFile
            if(!parentFile.exists()){
              parentFile.mkdirs()
            }
            if(!fileDest.exists()){
              fileDest.createNewFile()
            }
            println(file.renameTo(fileDest))
          }
        }
      }
    }


  }
}
