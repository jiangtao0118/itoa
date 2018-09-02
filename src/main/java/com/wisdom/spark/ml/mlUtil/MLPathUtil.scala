package com.wisdom.spark.ml.mlUtil

/**
  * Created by wisdom on 2016/11/17.
  * tgt: String, 预测指标
  * hostnode: String, 主机节点
  */
object MLPathUtil {
  val mlRootPath = PropertyUtil.getProperty("mlRootPath")
  //val mlRootPath = ContextUtil.mlRootPath

  /**
    *
    * @param tgt 取值DB2DIAG
    * @param hostnode 主机节点
    * @return
    */
  def db2diagCleanLogPath(tgt:String,hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/db2diag_clean.txt"
  }

  /**
    *
    * @param tgt 取值DB2DIAG
    * @param hostnode 主机节点
    * @return
    */
  def db2diagTFIDFModelPath(tgt:String,hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/tfidfModel"
  }

  /**
    * 标准化模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def scalerModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/scaler/model/"
  }

  /**
    * 标准化模型处理后的数据存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def scalerDataPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/scaler/data/"
  }

  /**
    * gmm模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def gmmModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/gmm/model/"
  }

  /**
    * 移动窗口统计模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def statsModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/stats/model/"
  }

  /**
    * PCA模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def pcaModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/pca/model/"
  }

  /**
    * MLP模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def mlpModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mlp/model/"
  }

  /**
    * PCA模型处理后的数据存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def pcaDataPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/pca/data/"
  }

  /**
    * RandomForest模型存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def rfmModelPath(tgt: String, hostnode: String): String = {
    mlRootPath + "/" + tgt + "/" + hostnode + "/rfm/model/"
  }

  /**
    * 相关系数矩阵方法降维后数据存储路径
    * @param tgt
    * @param hostnode
    * @return
    */
  def corrDataPath(tgt: String, hostnode: String): String = {
//    mlRootPath + "/" + tgt + "/" + hostnode + "/corr/"
    mlRootPath + "/" + tgt + "/" + hostnode + "/mid/corr/"
  }

  def featureColsKey(tgt: String, hostnode: String): String = {
    tgt + "." + hostnode + ".features"
  }

  def corrFeatureColsKey(tgt: String, hostnode: String): String = {
    tgt + "." + hostnode + ".corr.features"
  }

}
