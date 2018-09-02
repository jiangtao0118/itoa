package com.wisdom.spark.ml.db

import java.sql.{Connection, PreparedStatement}
import java.util

import com.wisdom.spark.common.util.ConnPoolUtil2
import com.wisdom.spark.ml.mlUtil.ContextUtil
import com.wisdom.spark.ml.tgtVar.GMMPredictor.GMMTrainParams
import com.wisdom.spark.streaming.tools.ConnUtils

/**
  * Created by tup on 2017/3/24.
  */
class GMMTrainParamDao {

  val logger = ContextUtil.logger
  def saveTrainParamList2Mysql(list: util.ArrayList[GMMTrainParams]): Unit = {
    if (list != null && list.size != 0) {
      var pst: PreparedStatement = null
      var conn: Connection = null
      val sql = "insert into wsd.T_MODEL_TRAIN_TEST_PARAS ( " +
        " tgtvar           " +
        ",hostnode         " +
        ",period           " +
        ",corrThreshold    " +
        ",clusters         " +
        ",maxIter          " +
        ",tolerance        " +
        ",seed             " +
        ",betaMin          " +
        ",betaStep         " +
        ",betaMax          " +
        ",trainFileURI     " +
        ",trainRecordCnt   " +
        ",trainCorrCost    " +
        ",traindmCnt       " +
        ",trainCost        " +
        ",testFileURI      " +
        ",testOutputFileURI" +
        ",testRecordCnt    " +
        ",testCost         " +
        ",testCorr         " +
        ",testMAE          " +
        ",testMAER         " +
        ",testSSE          " +
        //       ",recordTime       "  +
        ",reserveCol1      " +
        ",reserveCol2      " +
        ",reserveCol3      " +
        ",reserveCol4      " +
        ",reserveCol5      " +
        ") " +
        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      try {
        conn = ConnPoolUtil2.getConn()
        pst = conn.prepareStatement(sql)
        for (i <- 0 until list.size()) {
          val tgt_ind = list.get(i).tgtVar.substring(0, list.get(i).tgtVar.lastIndexOf("_"))
          val tgt_int = list.get(i).tgtVar.substring(list.get(i).tgtVar.lastIndexOf("_") + 1)
          pst.setString(1, tgt_ind)
          pst.setString(2, list.get(i).hostnode)
          pst.setString(3, tgt_int)
          pst.setFloat(4, list.get(i).trainParam.corrThreshold.toFloat)
          pst.setInt(5, list.get(i).trainParam.clusters)
          pst.setInt(6, list.get(i).trainParam.maxIter)
          pst.setFloat(7, list.get(i).trainParam.tolerance.toFloat)
          pst.setLong(8, list.get(i).trainParam.seed)
          pst.setFloat(9, list.get(i).trainParam.betaMin.toFloat)
          pst.setFloat(10, list.get(i).trainParam.betaStep.toFloat)
          pst.setFloat(11, list.get(i).trainParam.betaMax.toFloat)
          pst.setString(12, list.get(i).trainParam.trainFile)
          pst.setLong(13, list.get(i).trainParam.trainRecordCnt)
          pst.setLong(14, list.get(i).trainParam.trainCorrCost)
          pst.setInt(15, list.get(i).trainParam.traindmCnt)
          pst.setLong(16, list.get(i).trainParam.trainCost)
          pst.setString(17, list.get(i).trainParam.testFile)
          pst.setString(18, list.get(i).testParam.preOutPath)
          pst.setLong(19, list.get(i).testParam.testRecordCnt)
          pst.setLong(20, list.get(i).testParam.testCost)
          pst.setFloat(21, list.get(i).testParam.testCorr.toFloat)
          pst.setFloat(22, list.get(i).testParam.testMAE.toFloat)
          pst.setFloat(23, list.get(i).testParam.testMAER.toFloat)
          pst.setFloat(24, list.get(i).testParam.testSSE.toFloat)
          //保存筛选出的字段，目前MySQL中字段长度是 VARCHAR 3000
          val cols = list.get(i).trainParam.corrCols.mkString(",")
          pst.setString(25, cols.substring(0,math.min(cols.length,3000)))
          pst.setString(26, "")
          pst.setString(27, "")
          pst.setString(28, "")
          pst.setString(29, "")
          pst.addBatch()
        }
        pst.executeBatch()
      } catch {
        case e: Exception => logger.error("SQL Exception!: " + e.getMessage)
      } finally {
        ConnUtils.closeStatement(null, pst, null)
        ConnPoolUtil2.releaseCon(conn)
      }
    }
  }

}
