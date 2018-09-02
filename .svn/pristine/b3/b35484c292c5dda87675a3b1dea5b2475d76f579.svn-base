package com.wisdom.spark.ml.features

import com.wisdom.spark.ml.model.ft.{Complex, DFT}
import org.apache.commons.math3.distribution.NormalDistribution

/**
  * Created by tup on 2017/3/6.
  */
object Smoothing1D {

  def main(args: Array[String]) {
    val times1 = "96,95,96,96,96,96,96,96,96,96,96,97," +
      "97,97,97,97,97,97,97,97,97,97,97,97,98,97,98," +
      "97,97,97,98,97,98,98,98,98,98,93,97,97,97,96," +
      "97,98,98,98,98,98,98,98,98,98,98,98,98,98,98," +
      "98,98,98,98,98,98,98,97,98,98,97,97,97,97,97," +
      "97,97,96,97,96,96,96,96,96,96,95,95,95,95,95," +
      "95,95,94,95,93,94,94,92,93,93,93,90,90,88,87," +
      "86,83,83,84,82,83,80,78,86,79,82,80,78,77,80," +
      "81,76,75,75,81,78,77,77,76,79,76,74,77,75,79," +
      "71,73,73,72,73,80,83,78,76,81,82,80,81,82,88," +
      "81,81,84,82,84,80,80,85,82,82,82,87,83,79,80," +
      "80,74,81,75,82,77,78,79,74,83,77,74,78,71,69," +
      "73,77,74,78,71,77,73,80,79,77,79,77,77,76,83," +
      "78,78,81,82,83,79,77,79,82,84,81,82,82,87,84," +
      "86,84,82,87,86,83,88,86,87,83,86,88,88,88,89," +
      "87,89,88,86,88,90,90,89,89,88,89,89,89,90,88," +
      "88,86,88,89,90,90,89,86,91,89,86,89,90,89,89," +
      "87,88,87,88,90,88,90,88,90,89,90,90,91,87,91," +
      "91,92,90,91,91,91,91,93,93,93,92,93,94,94,92," +
      "94,95,94,95,95,95"
    val times = times1.split(",").map(x => x.toDouble)

//    val out = gaussianblur(2, 1.5, times)
    val out = fourierSmooth(8,times)
//    val out = exponentialSmooth(0.5,times)
    println(out.mkString(","))
  }

  //平均值
  //中位数
  //最值平滑法

  /**
    * 指数平滑
    * @param alpha 指数参数
    * @param data
    * @return
    */
  def exponentialSmooth(alpha: Double,data: Array[Double]): Array[Double] = {
    val out = new Array[Double](data.length)
    out(0) = data(0)
    for(i<-1 until data.length){
      out(i) = alpha * data(i) + (1-alpha) * out(i-1)
    }
    out
  }

  /**
    * 复数傅立叶变换滤波平滑
    * @param k 保留前k个低频波，从0开始
    * @param data 要平滑的数据
    * @return 平滑后的数据
    */
  def fourierSmooth(k: Int, data: Array[Double]): Array[Double] = {
    val fts = DFT.dft(data)
    //    val apts = new Array[Double](fts.length)
    //    val phase = new Array[Double](fts.length)
    //    for(i<-0 until fts.length){
    //      val re = fts(i).re()
    //      val im = fts(i).im()
    //      apts(i)=math.sqrt(re*re + im*im)
    //      phase(i)=math.atan2(im,re)
    //    }
    //
    //    println("原数据," + times)
    //    println("振幅," + apts.mkString(","))
    //    println("相位," + phase.mkString(","))

    val nfts = fts.zipWithIndex.map(x =>
      if (x._2 > k) {
        new Complex(0, 0)
      } else {
        if(x._2==0){
          x._1
        } else{
          //滤波时同时丢失了振幅（“能量”），利用复数傅立叶变换的共轭对称性，保留对称部分能量
          new Complex(x._1.re()*2, x._1.im()*2)
        }

      })

    val ifts=DFT.idft(nfts).map(x=>x.re())
    ifts
  }

  /**
    *
    * @param k     窗口长度，对应1*sigma、2*sigma、...、k*sigma点，
    * @param sigma 标准差
    * @param data  要平滑的数据
    * @return 平滑后的数组，未做边缘平滑
    */
  def gaussianblur(k: Int, sigma: Double, data: Array[Double]): Array[Double] = {
    val out = new Array[Double](data.length)
    val dis = new NormalDistribution(0, sigma)
    var weights = new Array[Double](k + 1)
    weights(0) = dis.density(0)
    for (i <- 1 until weights.length) {
      weights(i) = dis.density(i * sigma)
    }
    val sum = weights(0) + weights.drop(1).sum * 2
    weights = weights.map(x => x / sum)

    for (i <- 0 until data.length) {
      var s = data(i) * weights(0)
      if (i < k || i >= data.length - k) {
        s = data(i)
      }
      else {
        for (w <- 1 until (k + 1)) {
          s += (data(i - w) + data(i + w)) * weights(w)
        }
      }

      out(i) = s
    }
    out
  }


}
