/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.clustering

import scala.collection.mutable.IndexedSeq
import breeze.linalg.{MatrixNotSymmetricException, diag, DenseMatrix => BreezeMatrix, DenseVector => BDV, Vector => BV}
import breeze.numerics.abs
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import com.wisdom.spark.ml.mlUtil.{ContextUtil, HDFSFileUtil, LogUtil}

/**
  * :: Experimental ::
  *
  * This class performs expectation maximization for multivariate Gaussian
  * Mixture Models (GMMs).  A GMM represents a composite distribution of
  * independent Gaussian distributions with associated "mixing" weights
  * specifying each's contribution to the composite.
  *
  * Given a set of sample points, this class will maximize the log-likelihood
  * for a mixture of k Gaussians, iterating until the log-likelihood changes by
  * less than convergenceTol, or until it has reached the max number of iterations.
  * While this process is generally guaranteed to converge, it is not guaranteed
  * to find a global optimum.
  *
  * Note: For high-dimensional data (with many features), this algorithm may perform poorly.
  *       This is due to high-dimensional data (a) making it difficult to cluster at all (based
  *       on statistical/theoretical arguments) and (b) numerical issues with Gaussian distributions.
  *
  * @param k The number of independent Gaussians in the mixture model
  * @param convergenceTol The maximum change in log-likelihood at which convergence
  * is considered to have occurred.
  * @param maxIterations The maximum number of iterations to perform
  */
@Experimental
@Since("1.3.0")
class JGM private(
                                private var k: Int,
                                private var convergenceTol: Double,
                                private var maxIterations: Int,
                                private var seed: Long) extends Serializable {

  @transient
  val logger = ContextUtil.logger

  /**
    * Constructs a default instance. The default parameters are {k: 2, convergenceTol: 0.01,
    * maxIterations: 100, seed: random}.
    */
  @Since("1.3.0")
  def this() = this(2, 0.01, 100, Utils.random.nextLong())

  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5

  // an initializing GMM can be provided rather than using the
  // default random starting point
  private var initialModel: Option[JGMM] = None

  /**
    * Set the initial GMM starting point, bypassing the random initialization.
    * You must call setK() prior to calling this method, and the condition
    * (model.k == this.k) must be met; failure will result in an IllegalArgumentException
    */
  @Since("1.3.0")
  def setInitialModel(model: JGMM): this.type = {
    if (model.k == k) {
      initialModel = Some(model)
    } else {
      throw new IllegalArgumentException("mismatched cluster count (model.k != k)")
    }
    this
  }

  /**
    * Return the user supplied initial GMM, if supplied
    */
  @Since("1.3.0")
  def getInitialModel: Option[JGMM] = initialModel

  /**
    * Set the number of Gaussians in the mixture model.  Default: 2
    */
  @Since("1.3.0")
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /**
    * Return the number of Gaussians in the mixture model
    */
  @Since("1.3.0")
  def getK: Int = k

  /**
    * Set the maximum number of iterations to run. Default: 100
    */
  @Since("1.3.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
    * Return the maximum number of iterations to run
    */
  @Since("1.3.0")
  def getMaxIterations: Int = maxIterations

  /**
    * Set the largest change in log-likelihood at which convergence is
    * considered to have occurred.
    */
  @Since("1.3.0")
  def setConvergenceTol(convergenceTol: Double): this.type = {
    this.convergenceTol = convergenceTol
    this
  }

  /**
    * Return the largest change in log-likelihood at which convergence is
    * considered to have occurred.
    */
  @Since("1.3.0")
  def getConvergenceTol: Double = convergenceTol

  /**
    * Set the random seed
    */
  @Since("1.3.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
    * Return the random seed
    */
  @Since("1.3.0")
  def getSeed: Long = seed

  /**
    * Perform expectation maximization
    */
  @Since("1.3.0")
  def run(data: RDD[Vector]): JGMM = {
    val sc = data.sparkContext

    // we will operate on the data as breeze data
    val breezeData = data.map(_.toBreeze).cache()

    // Get length of the input vectors
    val d = breezeData.first().length

    val shouldDistributeGaussians = JGM.shouldDistributeGaussians(k, d)

    // Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples
    val (weights, gaussians) = initialModel match {
      case Some(gmm) => (gmm.weights, gmm.gaussians)

      case None => {
        val samples = breezeData.takeSample(withReplacement = true, k * nSamples, seed)
        (Array.fill(k)(1.0 / k), Array.tabulate(k) { i =>
          val slice = samples.view(i * nSamples, (i + 1) * nSamples)
          new MultivariateGaussian(vectorMean(slice), initCovariance(slice))
        })
      }
    }

    var llh = Double.MinValue // current log-likelihood
    var llhp = 0.0            // previous log-likelihood

    var iter = 0
    while (iter < maxIterations && math.abs(llh-llhp) > convergenceTol) {
      // create and broadcast curried cluster contribution function
      val compute = sc.broadcast(JExpectationSum.add(weights, gaussians)_)

      // aggregate the cluster contribution for all sample points
      val sums = breezeData.aggregate(JExpectationSum.zero(k, d))(compute.value, _ += _)

      // Create new distributions based on the partial assignments
      // (often referred to as the "M" step in literature)
      val sumWeights = sums.weights.sum


      if (shouldDistributeGaussians) {
//        val numPartitions = math.min(k, 1024)
        val numPartitions = math.min(k, 1024)
        val tuples =
          Seq.tabulate(k)(i => (sums.means(i), sums.sigmas(i), sums.weights(i)))
        val (ws, gs) = sc.parallelize(tuples, numPartitions).map { case (mean, sigma, weight) =>
          updateWeightsAndGaussians(mean, sigma, weight, sumWeights)
        }.collect().unzip
        Array.copy(ws.toArray, 0, weights, 0, ws.length)
        Array.copy(gs.toArray, 0, gaussians, 0, gs.length)
      } else {
        var i = 0
        while (i < k) {
          val (weight, gaussian) =
            updateWeightsAndGaussians(sums.means(i), sums.sigmas(i), sums.weights(i), sumWeights)
          weights(i) = weight
          gaussians(i) = gaussian
          i = i + 1
        }
      }

      llhp = llh // current becomes previous
      llh = sums.logLikelihood // this is the freshly computed log-likelihood
      iter += 1
      logger.info("log-likelihood=================>" + llh)

    }

    new JGMM(weights, gaussians)
  }

  @Since("1.3.0")
  def runDAEM(data: RDD[Vector],
              bMin: Double,
              bStep: Double,
              bMax: Double): JGMM = {

    val stTime = System.currentTimeMillis()

    val sc = data.sparkContext

    // we will operate on the data as breeze data
    val breezeData = data.map(_.toBreeze).cache()

    // Get length of the input vectors
    val d = breezeData.first().length

    val shouldDistributeGaussians = JGM.shouldDistributeGaussians(k, d)

    // Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples
    val (weights, gaussians) = initialModel match {
      case Some(gmm) => (gmm.weights, gmm.gaussians)

      case None => {
        val samples = breezeData.takeSample(withReplacement = true, k * nSamples, seed)
        (Array.fill(k)(1.0 / k), Array.tabulate(k) { i =>
          val slice = samples.view(i * nSamples, (i + 1) * nSamples)
          new MultivariateGaussian(vectorMean(slice), initCovariance(slice))
        })
      }
    }

//    var llh = Double.MinValue // current log-likelihood
//    var llhp = 0.0            // previous log-likelihood

    //Deterministic Anti-Annealing Expectation-Maximization

    val betaMin = bMin
    val betaStep = bStep
    val betaMax = bMax

//    val betaMin = 0.2
//    val betaStep = 0.2
//    val betaMax = 1.8

    var iter = 0
    var betaIter = 0
    val betaIters = math.round((betaMax-1)/betaStep*2 + (1-betaMin)/betaStep).toInt
    var reachMax = false
    var beta = betaMin - betaStep
    while (betaIter <= betaIters) {

      betaIter += 1
      if(!reachMax){
        beta += betaStep
        if(math.abs(beta-betaMax)< 1e-7){
          reachMax = true
        }
      }
      else{
        beta -= betaStep
      }


    //Deterministic Annealing Expectation-Maximization
//    var iter = 0
//    val betaNums = 1
//    val step = 1.0/betaNums
//    var beta = 0.0
//
//    while(beta <= 1+1e-7){
//      beta += step

      logger.info("DAEM beta=================>" + beta)

      var llh = Double.MinValue // current log-likelihood
      var llhp = 0.0            // previous log-likelihood

      while (iter < maxIterations && math.abs(llh-llhp) > convergenceTol) {

        // create and broadcast curried cluster contribution function
        val compute = sc.broadcast(JExpectationSumDAEM.add(weights, gaussians, beta)_)

        // aggregate the cluster contribution for all sample points
        val sums = breezeData.aggregate(JExpectationSumDAEM.zero(k, d))(compute.value, _ += _)
        val sums2 = breezeData.aggregate(JExpectationSum.zero(k, d))(compute.value, _ += _)

        // Create new distributions based on the partial assignments
        // (often referred to as the "M" step in literature)
        val sumWeights = sums.weights.sum


        if (shouldDistributeGaussians) {
          //        val numPartitions = math.min(k, 1024)
          val numPartitions = math.min(k, 1024)
          val tuples =
            Seq.tabulate(k)(i => (sums.means(i), sums.sigmas(i), sums.weights(i)))
          val (ws, gs) = sc.parallelize(tuples, numPartitions).map { case (mean, sigma, weight) =>
            updateWeightsAndGaussians(mean, sigma, weight, sumWeights)
          }.collect().unzip
          Array.copy(ws.toArray, 0, weights, 0, ws.length)
          Array.copy(gs.toArray, 0, gaussians, 0, gs.length)
        } else {
          var i = 0
          while (i < k) {
            val (weight, gaussian) =
              updateWeightsAndGaussians(sums.means(i), sums.sigmas(i), sums.weights(i), sumWeights)
            weights(i) = weight
            gaussians(i) = gaussian
            i = i + 1
          }
        }

        llhp = llh // current becomes previous
        llh = sums.logLikelihood // this is the freshly computed log-likelihood
        iter += 1

//        if(iter%5==0){
//
//          for(z<-0 until gaussians.length){
//            logger.info(iter + ":mu=================>" + gaussians(z).mu.toArray.mkString(","))
//          }
//        }

        logger.info("log-likelihood=================>" + sums2.logLikelihood)

      }


    }

    val endTime = System.currentTimeMillis()

    logger.info("training parameter=================>"+breezeData.count+","+d+","+k+","+(endTime-stTime)/1000+","+iter+","+maxIterations+","+convergenceTol)

    new JGMM(weights, gaussians)
  }

  /**
    * Java-friendly version of [[run()]]
    */
  @Since("1.3.0")
  def run(data: JavaRDD[Vector]): JGMM = run(data.rdd)

  private def updateWeightsAndGaussians(
                                         mean: BDV[Double],
                                         sigma: BreezeMatrix[Double],
                                         weight: Double,
                                         sumWeights: Double): (Double, MultivariateGaussian) = {
    val mu = (mean /= weight)
    BLAS.syr(-weight, Vectors.fromBreeze(mu),
      Matrices.fromBreeze(sigma).asInstanceOf[DenseMatrix])
    val newWeight = weight / sumWeights

    //处理因计算精度问题导致的非对称矩阵异常
    //https://github.com/scalanlp/breeze/issues/356
    //https://github.com/scalanlp/breeze/commit/6a5985ac3774cde333ebd7353c7a08394553561d
    val tol = 1e-7
    val newSigma = sigma / weight
    for (i <- 0 until newSigma.rows; j <- 0 until i){
//      if (abs(newSigma(i,j) -  newSigma(j,i)) > abs(newSigma(i,j)) * tol ){
//        throw new MatrixNotSymmetricException
//      }
//      else{
//        newSigma(i,j) = newSigma(j,i)
//      }

      newSigma(i,j) = newSigma(j,i)

    }

    val newGaussian = new MultivariateGaussian(mu, newSigma)
    (newWeight, newGaussian)
  }

  /** Average of dense breeze vectors */
  private def vectorMean(x: IndexedSeq[BV[Double]]): BDV[Double] = {
    val v = BDV.zeros[Double](x(0).length)
    x.foreach(xi => v += xi)
    v / x.length.toDouble
  }

  /**
    * Construct matrix where diagonal entries are element-wise
    * variance of input vectors (computes biased variance)
    */
  private def initCovariance(x: IndexedSeq[BV[Double]]): BreezeMatrix[Double] = {
    val mu = vectorMean(x)
    val ss = BDV.zeros[Double](x(0).length)
    x.foreach(xi => ss += (xi - mu) :^ 2.0)
    diag(ss / x.length.toDouble)
  }
}

private[clustering] object JGM {
  /**
    * Heuristic to distribute the computation of the [[MultivariateGaussian]]s, approximately when
    * d > 25 except for when k is very small.
    *
    * @param k  Number of topics
    * @param d  Number of features
    */
  def shouldDistributeGaussians(k: Int, d: Int): Boolean = ((k - 1.0) / k) * d > 25
}

// companion class to provide zero constructor for JExpectationSum
private object JExpectationSum {
  def zero(k: Int, d: Int): JExpectationSum = {
    new JExpectationSum(0.0, Array.fill(k)(0.0),
      Array.fill(k)(BDV.zeros(d)), Array.fill(k)(BreezeMatrix.zeros(d, d)))
  }

  // compute cluster contributions for each input point
  // (U, T) => U for aggregation
  def add(
           weights: Array[Double],
           dists: Array[MultivariateGaussian])
         (sums: JExpectationSum, x: BV[Double]): JExpectationSum = {
    val p = weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(x)
//      case (weight, dist) => MLUtils.EPSILON + weight * math.exp(dist.logpdf(x) / 10)
    }
    val pSum = p.sum

    sums.logLikelihood += math.log(pSum)
    var i = 0
    while (i < sums.k) {
      p(i) /= pSum
      sums.weights(i) += p(i)
      sums.means(i) += x * p(i)
      BLAS.syr(p(i), Vectors.fromBreeze(x),
        Matrices.fromBreeze(sums.sigmas(i)).asInstanceOf[DenseMatrix])
      i = i + 1
    }
    sums
  }
}

// companion class to provide zero constructor for JExpectationSum
private object JExpectationSumDAEM {
  def zero(k: Int, d: Int): JExpectationSum = {
    new JExpectationSum(0.0, Array.fill(k)(0.0),
      Array.fill(k)(BDV.zeros(d)), Array.fill(k)(BreezeMatrix.zeros(d, d)))
  }

  // compute cluster contributions for each input point
  // (U, T) => U for aggregation
  def add(
           weights: Array[Double],
           dists: Array[MultivariateGaussian],
           beta: Double)
         (sums: JExpectationSum, x: BV[Double]): JExpectationSum = {
    val p = weights.zip(dists).map {
//      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(x)
//      case (weight, dist) => MLUtils.EPSILON + weight * math.exp(dist.logpdf(x) / 10)

      case (weight, dist) => MLUtils.EPSILON + math.pow(weight * dist.pdf(x),beta)
//      case (weight, dist) => MLUtils.EPSILON + math.pow(weight * math.exp(dist.logpdf(x) / 10),beta)
    }
    val pSum = p.sum

    sums.logLikelihood += math.log(pSum)
    var i = 0
    while (i < sums.k) {
      p(i) /= pSum
      sums.weights(i) += p(i)
      sums.means(i) += x * p(i)
      BLAS.syr(p(i), Vectors.fromBreeze(x),
        Matrices.fromBreeze(sums.sigmas(i)).asInstanceOf[DenseMatrix])
      i = i + 1
    }
    sums
  }
}

// Aggregation class for partial expectation results
private class JExpectationSum(
                              var logLikelihood: Double,
                              val weights: Array[Double],
                              val means: Array[BDV[Double]],
                              val sigmas: Array[BreezeMatrix[Double]]) extends Serializable {

  val k = weights.length

  def +=(x: JExpectationSum): JExpectationSum = {
    var i = 0
    while (i < k) {
      weights(i) += x.weights(i)
      means(i) += x.means(i)
      sigmas(i) += x.sigmas(i)
      i = i + 1
    }
    logLikelihood += x.logLikelihood
    this
  }
}
