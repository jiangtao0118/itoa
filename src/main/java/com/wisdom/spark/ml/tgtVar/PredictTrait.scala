package com.wisdom.spark.ml.tgtVar

/**
  * Created by tup on 2017/4/25.
  */
trait PredictTrait {

  def predict(data: Array[Double]): Array[Double]

}
