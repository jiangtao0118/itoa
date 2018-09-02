package com.wisdom.spark.ml.model.tfidf

import java.util

import com.wisdom.spark.ml.mlUtil._
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
  * Created by tup on 2017/1/24.
  */
object TFIDF {

  private case class log_sentence(text: String, rawWords: Array[String])
  def trainAndSave(inPath: String,
                   splitRegx: String,
                   removeRegx: Array[String],
                   removeWords: Array[String],
                   modelPath: String,
                   topTFIDFcnt: Int): TFIDFModel = {

    val sqlCtxt = ContextUtil.sqlContext
    import sqlCtxt.implicits._

    val clean_log = ContextUtil.sc.textFile(inPath)
    val logDF = clean_log.map(x => log_sentence(x, null)).toDF()
    val tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("rawWords").setPattern(splitRegx)

    val wordsData = tokenizer.transform(logDF.select("text"))
      .map(x => (x.getString(0), filterWordsArr(x.getSeq[String](1).toArray, removeRegx)))
      .map(x => log_sentence(x._1, x._2)).toDF()

    val stopWordsRemover = new StopWordsRemover()
    val stopWords = stopWordsRemover.getStopWords ++ removeWords
    // remove stop words
    val modi_input = stopWordsRemover
      .setInputCol("rawWords")
      .setOutputCol("cleanedWords")
      .setStopWords(stopWords)
      .transform(wordsData)

    // Using CountVectorizer to avoid collisions
    // can also set minTF/minDF
    val tfModel = new CountVectorizer()
      .setInputCol("cleanedWords")
      .setOutputCol("TFvaluesCnt")
//      .setMinDF(1)
      .fit(modi_input)
    val vocab = tfModel.vocabulary
    val tfDataCnt = tfModel.transform(modi_input)

    val binaryCnt : (Vector=>Vector) = (vec: Vector) => {
      val sparseV = vec.toSparse
      val indices = sparseV.indices
      val ones = new Array[Double](indices.length).map(x=>1.0)
      new SparseVector(vec.size,indices,ones)
    }
    val sqlfunc = udf(binaryCnt)

    val tfData = tfDataCnt.withColumn("TFvalues",sqlfunc(col("TFvaluesCnt")))

    val wordCntMap = tfData.select("TFvalues").map({
      x =>
        val sparseV = x.getAs[Vector](0).toSparse
        val indices = sparseV.indices
        val vocabMap = scala.collection.mutable.Map[String, Int]()
        for (i <- 0 until indices.length) {
          vocabMap.put(vocab(indices(i)), sparseV(indices(i)).toInt)
        }
        vocabMap
    }).reduce({
      (x, y) =>
        x ++ y.map { case (k, v) => k -> (v + x.getOrElse(k, 0)) }
    })

    val wordcnt = new Array[Int](vocab.length)
    for(i<-0 until vocab.length){
      wordcnt(i) = wordCntMap.getOrElse(vocab(i),0)
    }

    val idf = new IDF().setInputCol("TFvalues").setOutputCol("TFIDFvalues")
    val idfModel = idf.fit(tfData)
    /**
      * root
      * |-- text: string (nullable = true)
      * |-- rawWords: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * |-- cleanedWords: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * |-- TFvaluesCnt: vector (nullable = true)
      * |-- TFvalues: vector (nullable = true)
      * |-- TFIDFvalues: vector (nullable = true)
      */
    val idfData = idfModel.transform(tfData)

    val tfidfrdd = idfData.select("TFIDFvalues")
      .map { x: Row => x.getAs[Vector](0) }
      .map(x => x.toSparse)
      .map {
        x =>
          x.indices.zip(x.values)
            .map(y => (vocab(y._1), y._2))
            .sortBy(x=>(-x._2,x._1))
            .take(topTFIDFcnt)
      }
      .map(x => x.map(y => y._1))
      .map(x => x.mkString(","))

    //不同的关键词词组及其频次
    val arrTFIDF = tfidfrdd.map(x => (x, 1)).reduceByKey(_ + _).collect()
    val lowFreqWords = new util.HashMap[String, Int]()
    for(i<-0 until arrTFIDF.length){
      lowFreqWords.put(arrTFIDF(i)._1,arrTFIDF(i)._2)
    }

    val model = new TFIDFModel(
      PropertyUtil.getProperty("ml.db2diag.splitRegx"),
      removeRegx,
      removeWords,
      vocab,
      wordcnt,
      idfData.select("TFvalues","TFIDFvalues"),
      topTFIDFcnt,
      lowFreqWords
    )
    model.tfidfDF_=(idfData.select("TFvalues","TFIDFvalues"))

    TFIDFModel.save(modelPath,model)

    model

  }

  private def filterWordsArr(words: Array[String], removeRegx: Array[String]): Array[String] = {
    words.filter {
      y =>
        var tmpBool = false
        for (i <- 0 until removeRegx.length if (!tmpBool)) {
          tmpBool = removeRegx(i).r.findFirstIn(y).nonEmpty
        }
        !tmpBool
    }
  }

}



class TFIDFModel (
                  splitRegx: String,
                  removeRegx: Array[String],
                  stopWords: Array[String],
                  vocab: Array[String],
                  wordcnt: Array[Int],
                  tfidfData: DataFrame,
                  topTFIDFCnt: Int,
                  lowFreqWords: util.HashMap[String, Int]
                ) extends Serializable{

  val splitRegrex = splitRegx
  val removeRegrex = removeRegx
  val stopwords = stopWords
  val vocabulary = vocab
  val wordcount = wordcnt
  val topTFIDF = topTFIDFCnt
  val wordsarrMap = lowFreqWords

  private val tfidfSchema = tfidfData.schema
  private val tfidfArray = tfidfData.collect()

  private[this] var _tfidfDF: DataFrame = null

  def tfidfDF: DataFrame = _tfidfDF

  def tfidfDF_=(value: DataFrame): Unit = {
    _tfidfDF = value
  }

}

object TFIDFModel {

  val modelFile = "tfidfModel.obj"
  def exists(modelPath:String): Boolean = {
    val fileUri = modelPath + "/" + modelFile
    HDFSFileUtil.exists(fileUri)
  }

  def save(modelPath:String,model: TFIDFModel): Unit = {
    val fileUri = modelPath + "/" + modelFile
    PersitObjectUtil.writeObjectToFile(model, fileUri)

  }

  def load(modelPath:String): TFIDFModel = {
    val fileUri = modelPath + "/" + modelFile
    val model = PersitObjectUtil.readObjectFromFile(fileUri).asInstanceOf[TFIDFModel]

    model.tfidfDF_=(
      ContextUtil.sqlContext.createDataFrame(
        ContextUtil.sc.parallelize(model.tfidfArray),
        model.tfidfSchema
      )
    )
    model
  }
}
