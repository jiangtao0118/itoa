package com.wisdom.spark.etl.util

import com.wisdom.spark.common.util.{ItoaPropertyUtil, SparkContextUtil}
import com.wisdom.spark.etl.ppn.DFT
import com.wisdom.spark.ml.mlUtil.ModelUtil
import com.wisdom.spark.streaming.thread.Thread4InitialModelObj
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.control.Breaks

/**
  * Created by htgeng on 2017/2/9.
  */
object SaveDFTModel {

  def main(args: Array[String]) {
    val properties=ItoaPropertyUtil.getProperties(args(0))
    val init=new InitUtil(properties,Thread4InitialModelObj.getModelMap())
    val sc=SparkContextUtil.getInstance()


    val files = sc.wholeTextFiles(init.commonInputPath+"*")
    files.foreach(x => saveDFTModel(init,x._1.substring(x._1.lastIndexOf("/")+1,x._1.lastIndexOf(".")), x._2.split("\n")))

    sc.stop()

  }

  def saveDFTModel(init:InitUtil,hostname:String,column_current_withTimestamp: Array[String]):Unit={
    val filePath: String = init.commonOutputPath + init.currentTableName + "/"+ hostname + "/"+ init.currentBean.getPredictionField + "/"  + hostname + ".txt"
    val path = new Path(filePath)
    val conf = new Configuration()
    //    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)

    val os = fs.create(path)
    val arr=new Array[Double](288*7)

    val breaks=new Breaks
    breaks.breakable(
      for(i <-  column_current_withTimestamp.indices){
        val row=column_current_withTimestamp(i)
        val cols=row.split(",")
        val firstPoint="2016"+cols(0)+cols(1)+cols(2)+cols(3)
        val firstPointMin="2016"+cols(0)+cols(1)+"00"+"00"
        val firstPointMax="2016"+cols(0)+cols(1)+"00"+"04"
        val isFirstPoint=FFTUtil.isFistOrLastPoint(firstPointMin,firstPoint,firstPointMax)
        //timeStamp is sunday
        if(isFirstPoint){
          var j=i+1
          var k=1

          val predictionFieldIndex=init.currentBean.getPredictionFieldIndex+4
          val actual=column_current_withTimestamp(i).split(",")(predictionFieldIndex)
          arr(0)=actual.toDouble

          while(j<= column_current_withTimestamp.length && j<i+288*7){
            val previousData=column_current_withTimestamp(j-1)
            val currentData=column_current_withTimestamp(j)
            var data=previousData
            if(FFTUtil.dataIsContinuous(previousData,currentData)){
              data=currentData
            }else{
              data=previousData
            }

            val predictionFieldIndex=init.currentBean.getPredictionFieldIndex+4
            val actual=data.split(",")(predictionFieldIndex)
            arr(k)=actual.toDouble
            j+=1
            k+=1
          }
          breaks.break()
        }
      }
    )


    val fftModels=DFT.dft(arr)

    for(i<- fftModels.indices){
      val fftModel=fftModels(i)
      os.write((fftModel.re()+","+fftModel.im()+arr(i)+"\n").getBytes())
    }
    os.close()
    fs.close()
  }
}
