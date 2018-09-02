package com.wisdom.spark.ml.tgtVar

import java.util

import com.wisdom.spark.ml.mlUtil._
import com.wisdom.spark.ml.model.tfidf.{TFIDF, TFIDFModel}
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.{Dbscan, DbscanSettings}
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.clustering.{HierarchicalClustering, KMeansCosine}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.ListMap

/**
  * Created by tup on 2017/1/13.
  */
object DB2Diag {

  @transient
  val logger = ContextUtil.logger
  val usage =
    """
Usage:spark-submit --master yarn-client [spark options] --class DB2Diag JSparkML.jar <command args>

<command args> :
  cleanLog inpath targetVar hostnode
      |inpath db2diag原始文件
      |targetVar 预测指标
      |hostnode 主机节点
  trainTFIDF targetVar hostnode topTFIDFcnt
      |targetVar 预测指标
      |hostnode 主机节点
      |topTFIDFcnt top个数
  checkDB2diag targetVar hostnode threshold
      |targetVar 预测指标
      |hostnode 主机节点
      |topTFIDFcnt top个数
    """

  def help(): Unit = {
    println(usage)
    System.exit(1)
  }

//  val tfidfModelPath = "C:\\Users\\wisdom\\Desktop\\tfidfModel"
//  val word2vecModelPath = "C:\\Users\\wisdom\\Desktop\\word2vecModel"
//  val removeWordsRegx = """S\d{7}\.LOG,0x.*,\d+,#\d+,-+""".split(",")
//  val removeWords = """data,DB2,UDB,#,",(,),=,/,+""".split(",")

  def main(args: Array[String]) {

    val sc = ContextUtil.sc

    @transient
    val logger = ContextUtil.logger
//
    val test=new DB2Diag("DB2DIAG","ASCECUP01")
    val record="2015-07-16-16.41.08.555250+480 I1537A404            LEVEL: Event\\nPID     : 14614742             TID : 1              PROC : db2diag\\nINSTANCE: db2iaops             NODE : 000\\nHOSTNAME: DBCAOPS01\\nEDUID   : 1\\nFUNCTION: DB2 UDB, RAS/PD component, pdDiagArchiveDiagnosticLog, probe:88\\nCREATE  : DB2DIAG.LOG ARCHIVE : /home/db2iaops/sqllib/db2dump/db2diag.log_2015-07-16-16.41.08 : success\\nIMPACT  : Potential"
    val out=test.lowfreq(record,'3')

    if (args.length == 0) {
      help()
    }

    val method = args(0)
    if (method.equalsIgnoreCase("cleanLog")) {

      if (args.length != 4) {
        help()
      }

      val inpath = args(1)
      val tgt = args(2)
      val hostnode = args(3)

      cleanLog(inpath,MLPathUtil.db2diagCleanLogPath(tgt,hostnode))
    }
    else if (method.equalsIgnoreCase("trainTFIDF")) {

      if (args.length != 4) {
        help()
      }

      val tgt = args(1)
      val hostnode = args(2)
      val topTFIDFcnt = args(3).toInt

      val inPath = MLPathUtil.db2diagCleanLogPath(tgt,hostnode)
      val modelPath = MLPathUtil.db2diagTFIDFModelPath(tgt,hostnode)

      val removeRegx = PropertyUtil.getProperty("ml.db2diag.removeRegx").split(",")
      val removeWords = PropertyUtil.getProperty("ml.db2diag.removeWords").split(",")

      toptfidf(
        inPath,
        modelPath,
        topTFIDFcnt,
        removeRegx,
        removeWords
      )

    }
    else if (method.equalsIgnoreCase("checkDB2diag")) {

      if (args.length != 4) {
        help()
      }
      val tgt = args(1)
      val hostnode = args(2)
      val threshold = args(3).toInt

      val diag = new DB2Diag(tgt,hostnode)

      val reader = HDFSFileUtil.read(MLPathUtil.db2diagCleanLogPath(tgt,hostnode), "ASCII")

      var line = reader.readLine()
      while (line != null) {

        diag.lowfreq(line,threshold)
//        logger.info(diag.lowfreq(line,threshold))
        line = reader.readLine()
      }

      HDFSFileUtil.closeReader(reader)

    }
    else if (method.equalsIgnoreCase("word2vec")) {

      if (args.length != 6) {
        help()
      }
    }
    else {
      help()
    }

    ContextUtil.sc.stop()

//    val tfidfrdd = toptfidf(
//      outFileURI,
//      tfidfModelPath,
//      5,
//      ",",
//      removeWordsRegx,
//      removeWords
//    )
//
//    //不同的关键词词组及其频次
//    val arrTFIDF = tfidfrdd.map(x => (x, 1)).reduceByKey(_ + _)
//    arrTFIDF.collect.foreach(println)
//
//    //        val model = trainWord2vecModel(10, outFileURI, word2vecModelPath,removeWords,removeWordsRegx)
//    val model = Word2VecModel.load(ContextUtil.sc, word2vecModelPath)
//
//    //    println(model.findSynonyms("sqlpgArchiveLogFile",10).mkString(","))
//    val wordvecMap = model.getVectors
//    //    synonymWords(wordvecMap,10,10000,54321.toLong,"C:\\Users\\wisdom\\Desktop\\wordCluster")
//
//
//    val sentenceVec = arrTFIDF.map(
//      x =>
//        (x._1, x._2, sentence2vec(wordvecMap, x._1.split(",")))
//    )
//    sentenceVec.collect().map(x => (x._1, x._2, x._3.mkString(","))).foreach(println)
//    //    synonymSentence(sentenceVec.map(x => (x._1, x._3)), 40, 1500, 54321.toLong, "C:\\Users\\wisdom\\Desktop\\sentenceCluster")
//
//    val unit = sentenceVec.map {
//      x =>
//        val norm2 = math.sqrt(x._3.map(y => y * y).sum)
//        (x._1, x._3.map(x => x / norm2))
//    }
//
////    synonymSentenceDBSCAN(unit, 0.32, 2, "C:\\Users\\wisdom\\Desktop\\sentenceCluster")
//
//    val HCmodel = HierarchicalClustering.train(
//      unit.map(x=>x._2).map(x=>Vectors.dense(x)),
//      40,
//      200,
//      10,
//      0.001,
//      1,
//      0.5
//    )
//
//    unit.map(x=>(x._1,HCmodel.predict(Vectors.dense(x._2)))).foreach(println)
//
//    println("Depth,Height,DataSize,Variance,Treesize")
//    HCmodel.clusterTree.getClusters().foreach{
//      x=>
//        println(x.getDepth() + "," + x.getDataSize().get + "," + x.getVariance() + "," + x.getTreeSize())
//    }
//
//    HCmodel.clusterTree.print

  }


  def sentence2vec(wordvecMap: Map[String, Array[Float]],
                   sentenceWords: Array[String]
                  ): Array[Float] = {
    sentenceWords
      .map({ y =>
        if (!wordvecMap.contains(y)) {
          logger.info("word2vec中不存在=============>" + y)
        }
        wordvecMap.get(y).get
      }
      )
      .reduce({
        (v1, v2) =>
          val rst = new Array[Float](v1.length)
          for (j <- 0 until rst.length) {
            rst(j) = v1(j) + v2(j)
          }
          rst
      })
  }

  def trainWord2vecModel(vecSize: Int,
                         inpath: String,
                         modelPath: String,
                         removeWords: Array[String],
                         removeRegx: Array[String]): Word2VecModel = {
    val clean_log = ContextUtil.sc.textFile(inpath)
    val logs = clean_log.map(_.split(",")).map(x => DB2DiagUtil.filterWordsArr(x, removeWords, removeRegx).toSeq)
    val word2vec = new Word2Vec()
      .setVectorSize(vecSize)
      .setMinCount(1)
    val model = word2vec.fit(logs)

    if (HDFSFileUtil.exists(modelPath)) {
      HDFSFileUtil.deleteFile(modelPath)
    }
    model.save(ContextUtil.sc, modelPath)

    model
  }

  def synonymWords(wordVecMap: Map[String, Array[Float]],
                   k: Int,
                   maxIter: Int,
                   seed: Long,
                   outPath: String) {

    val sqlCtxt = ContextUtil.sqlContext

    val kmeans = new KMeansCosine()
      .setK(k)
      .setMaxIterations(maxIter)
      .setRuns(3)
      .setSeed(seed)

    val wordvecRDD = ContextUtil.sc.parallelize(wordVecMap.toSeq)
    val vec = wordvecRDD.map(x => x._2.map(y => y.toDouble)).map(x => Vectors.dense(x))
    vec.cache()
    val model = kmeans.run(vec)
    //word,cluster,word vector
    val wordCluster = wordvecRDD.map(x => (x._1, model.predict(Vectors.dense(x._2.map(y => y.toDouble))), x._2))
    if (HDFSFileUtil.exists(outPath)) {
      HDFSFileUtil.deleteFile(outPath)
    }
    wordCluster.map(x => x._1 + "," + x._2).saveAsTextFile(outPath)

  }

  def synonymSentence(sentenceVec: RDD[(String, Array[Float])],
                      k: Int,
                      maxIter: Int,
                      seed: Long,
                      outPath: String) {

    val sqlCtxt = ContextUtil.sqlContext

    val kmeans = new KMeansCosine()
      .setInitializationMode(KMeansCosine.RANDOM)
      .setK(k)
      .setMaxIterations(maxIter)
      .setRuns(3)
      .setSeed(seed)

    val vec = sentenceVec.map(x => x._2.map(y => y.toDouble)).map(x => Vectors.dense(x))
    vec.cache()
    val model = kmeans.run(vec)
    //word,cluster,word vector
    val wordCluster = sentenceVec.map(x => (x._1, model.predict(Vectors.dense(x._2.map(y => y.toDouble)))))
    if (HDFSFileUtil.exists(outPath)) {
      HDFSFileUtil.deleteFile(outPath)
    }
    wordCluster.map(x => x._1 + "," + x._2).saveAsTextFile(outPath)

  }

  def synonymSentenceDBSCAN(sentenceVec: RDD[(String, Array[Double])],
                            epsilon: Double,
                            clusterPoints: Int,
                            outPath: String) {

    val clusteringSettings = new DbscanSettings()
      .withEpsilon(epsilon)
      .withNumberOfPoints(clusterPoints)
    //      .withDistanceMeasure(new CosineDistance())

    val vec = sentenceVec.map(x => new Point(x._2))

    val timeS1 = System.currentTimeMillis()

    val model = Dbscan.train(vec, clusteringSettings)

    val timeE1 = System.currentTimeMillis()
    logger.info("===>DBSCAN训练模型耗时：" + (timeE1 - timeS1) + "ms")


    //word,cluster,word vector
    val wordCluster = sentenceVec
      .collect()
      .map {
        x =>

          val timeS2 = System.currentTimeMillis()

          val clusterid = model.predict(new Point(x._2))

          val timeE2 = System.currentTimeMillis()
          logger.info("===>DBSCAN预测耗时：" + (timeE2 - timeS2) + "ms")


          (x._1, clusterid)
      }
    if (HDFSFileUtil.exists(outPath)) {
      HDFSFileUtil.deleteFile(outPath)
    }
    wordCluster.map(x => x._1 + "," + x._2).foreach(logger.info)

  }


  def toptfidf(logPath: String,
               modelPath: String,
               topTFIDF: Int,
               removeRegx: Array[String],
               removeWords: Array[String]): Unit = {

    val top = topTFIDF
    val tfidfmodel = {
      if (TFIDFModel.exists(modelPath)) {
        TFIDFModel.load(modelPath)
      }
      else {
        TFIDF.trainAndSave(
          logPath,
          ",",
          removeRegx,
          removeWords,
          modelPath,
          topTFIDF
        )
      }
    }

    val vocab = tfidfmodel.vocabulary
    val wordcnt = tfidfmodel.wordcount
    logger.info("分词个数============>" + vocab.length)
    logger.info("word count============>")
    for (i <- 0 until vocab.length) {
      logger.info(vocab(i) + "," + wordcnt(i))
    }

    /**
      * root
      * |-- text: string (nullable = true)
      * |-- rawWords: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * |-- cleanedWords: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * |-- TFvalues: vector (nullable = true)
      * |-- TFIDFvalues: vector (nullable = true)
      */

    logger.info("Top TFIDF words array============>")
    tfidfmodel.wordsarrMap.entrySet().toArray().foreach(logger.info)
  }

  //将多行日志合并为1行
  //按照正则表达式分词
  //结果输出到文件，以英文逗号分隔
  def cleanLog(inpath: String,
               outpath: String
              ): Unit = {

    val reader = HDFSFileUtil.read(inpath, "ASCII")
    val timeRegx = PropertyUtil.getProperty("ml.db2diag.timeRegx").r
    val splitRegx = PropertyUtil.getProperty("ml.db2diag.splitRegx")

    val strBuffer = new StringBuffer()
    if (HDFSFileUtil.exists(outpath)) {
      HDFSFileUtil.deleteFile(outpath)
    }
    val outStream = HDFSFileUtil.write(outpath, true)
    var tmpOut = ""
    var tmpTime: Option[String] = None
    var cnt = 0

    var line = reader.readLine()
    while (line != null) {

      if (line.trim.length > 0) {

        tmpTime = timeRegx.findFirstIn(line)
        if (tmpTime.nonEmpty) {
          cnt += 1
        }
        if (tmpTime.nonEmpty && cnt > 1) {
          tmpOut = DB2DiagUtil.cleanRecord(strBuffer.toString,splitRegx) + "\n"

          outStream.writeBytes(tmpOut)
          outStream.flush()
          strBuffer.delete(0, strBuffer.length())
        }

        strBuffer.append(line + " ")

      }

      line = reader.readLine()
    }

    HDFSFileUtil.closeSteam(outStream)
    HDFSFileUtil.closeReader(reader)
  }



}

object DB2DiagUtil{
  def cleanRecord(record: String, splitRegx: String): String = {

    record.replaceAll("""\s*:\s*|\"|\(|\)""", ":").split(splitRegx).filter(x => x.length > 0).mkString(",")
  }

  def filterWordsArr(words: Array[String],
                             removeWords: Array[String],
                             removeRegx: Array[String]): Array[String] = {
    words.filter {
      y =>
        !removeWords.map(x=>x.toLowerCase).contains(y.toLowerCase)
    }.filter {
      y =>
        var tmpBool = false
        for (i <- 0 until removeRegx.length if (!tmpBool)) {
          tmpBool = removeRegx(i).r.findFirstIn(y).nonEmpty
        }
        !tmpBool
    }
  }
}

class DB2Diag(targetVar: String, hostnode: String) extends Serializable {

//  @transient
//  val logger = ContextUtil.logger

  @transient
  val logger = Logger.getLogger(this.getClass)

  val tfidfmodelpath = MLPathUtil.db2diagTFIDFModelPath(targetVar,hostnode)

  val tfidfmodel = {
    if (TFIDFModel.exists(tfidfmodelpath)) {
      TFIDFModel.load(tfidfmodelpath)
    }
    else {
      null
    }
  }

  val modelSplitRegrex=PropertyUtil.getProperty("ml.db2diag.splitRegx")
  val stopWords = new StopWordsRemover().getStopWords ++ tfidfmodel.stopwords
  val stopRegx = tfidfmodel.removeRegrex

  def lowfreq(logrecord: String, threshold: Int): DB2diagOut = {

    logger.info("指标: " + targetVar + ", 节点: " + hostnode + ", 接收到实时数据: " + logrecord)

    val cleaned = DB2DiagUtil.cleanRecord(logrecord,modelSplitRegrex).split(",")
    logger.info("指标: " + targetVar + ", 节点: " + hostnode + ", 分词后数据: " + cleaned.mkString(","))

    val removed = DB2DiagUtil.filterWordsArr(cleaned,stopWords,stopRegx)
    logger.info("指标: " + targetVar + ", 节点: " + hostnode + ", 剔除StopWords: " + removed.mkString(","))

    val wordcnt = scala.collection.mutable.Map[String, (Int, Int)]()

    for(i<-0 until removed.length){
      val fq = wordcnt.get(removed(i))
      fq match {
        case Some((a,b)) => wordcnt.put(removed(i),(a+1,b))
        case None =>
          val idx = tfidfmodel.vocabulary.indexOf(removed(i))
          var cnt = 0
          if(idx > -1){
            cnt = tfidfmodel.wordcount(idx)
          }
          wordcnt.put(removed(i),(1,cnt))
      }
    }

    val idf = wordcnt.map{ case (k,v)=>(k,math.log(1.0/(1.0 + v._2)))}
    val topWords = idf.toSeq.sortBy(x=>(-x._2,x._1)).take(tfidfmodel.topTFIDF).map(x=>x._1)

    val cnt = tfidfmodel.wordsarrMap.getOrDefault(topWords.mkString(","),0)
    val out = cnt < threshold
//    logger.info("DB2DIAG日志时间戳:" + cleaned(0) + ", ID:" + cleaned(1) + ", 是否低频:" + out + ", 检验词:" + topWords.mkString(",") + ", 知识库频数:" + cnt )
    logger.info("DB2DIAG校验输出:" + hostnode + ":" + cleaned(0) + ":" + cleaned(1) + ":" + out + ":" + topWords.mkString(",") + ":" + cnt )

    val wordList = new util.ArrayList[(String,Int)]()
    for(i<-0 until topWords.length){
      wordList.add((topWords(i),wordcnt.get(topWords(i)).get._2))
    }
    DB2diagOut(hostnode,cleaned(0),cleaned(1),out,wordList,cnt)
  }

}

case class DB2diagOut (
                      hostnode: String, //主机名
                      db2Time: String, //日志时间戳 格式形如 2015-07-16-16.41.08.570656+480
                      db2ID: String, //日志ID 如 I141865A550
                      wordGroupIsLF: Boolean, //词组是否低频
                      wordGroupAndFreq: util.ArrayList[(String,Int)], //以英文逗号分隔的词,及每个词出现的频率, 可以用来判断每个词是否低频, 重要性由高到低排序
                      wordGroupFreq: Int //词组出现频率
                      )
