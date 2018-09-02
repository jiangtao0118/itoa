package com.wisdom.spark.ml.mlUtil

/**
  * Created by wisdom on 2016/6/6.
  */
import java.util.Random

object PiTest {
  def main(args: Array[String]) {

    /*
    * spark-submit --class PiTest --master local   /home/cloudera/SparkRemoteTest.jar
    * 如果需要在命令行中通过spark-submit提交任务，需要注销上面的System.setProperty和setMaster、setJars
    *
    * */
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = ContextUtil.sc.parallelize(1 to n, slices).map { i =>
      val x = new Random().nextFloat()
      val y = new Random().nextFloat()
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)

    Thread.sleep(100000)
    println("Pi is roughly " + count*4.0 / n)

  }
}
