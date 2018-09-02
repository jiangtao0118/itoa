package com.wisdom.spark.etl.ppn

import java.io._
import java.text.DecimalFormat
import java.util
import java.util.concurrent.Executors

import com.wisdom.spark.common.util.SparkContextUtil
import com.wisdom.spark.etl.util.HDFSFileUtil
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.control.Breaks

/**
  * Created by htgeng on 2017/2/23.
  */
@deprecated
object AddDFT {
  def main(args: Array[String]) {
    val filePath = args(0)
//    val predictType = args(1)

    val predictPoints = Array(1, 2, 3, 4)
//    if (predictType.equalsIgnoreCase("5")) {
//      val temp = Array(1, 3, 6, 12)
//      predictPoints = temp
//    } else if (predictType.equalsIgnoreCase("1")) {
//      val temp: Array[Int] = Array(5, 15, 30, 60)
//      predictPoints = temp
//    }

    val path = new Path(filePath)

    val sc = SparkContextUtil.getInstance()
    val fs = FileSystem.newInstance(sc.hadoopConfiguration)

    val pools = Executors.newFixedThreadPool(100)

    try {
      val hostFolders = HDFSFileUtil.listFiles(fs, path)

      for (i <- 0 until hostFolders.length) {
        val hostPath = hostFolders(i)
        if (fs.isDirectory(hostPath)) {
          val targetNameFolders = HDFSFileUtil.listFiles(fs, hostPath)
          val targetBreak = new Breaks
          targetBreak.breakable(
            for (j <- 0 until targetNameFolders.length) {
              val targetPath = targetNameFolders(j)
              if (fs.isDirectory(targetPath)) {
                val inputFiles = HDFSFileUtil.listFiles(fs, targetPath)
                var flag = false
                val break = new Breaks
                break.breakable(
                  for (k <- 0 until inputFiles.length) {
                    val inputFile = inputFiles(k)
                    val filePathTemp = inputFile
                    if (fs.isFile(filePathTemp) && inputFile.getName.contains("FT")) {
                      flag = true
                      break.break()
                    }
                  }
                )
                if (flag) {
                  targetBreak.break()
                }
                for (k <- 0 until inputFiles.length) {
                  val inputFile = inputFiles(k)
                  val filePathTemp = inputFile
                  if (fs.isFile(filePathTemp) && !inputFile.getName.contains("FT")&&inputFile.getName.endsWith(".csv")) {
                    println("-----targetName" + targetPath.getName + "_0" + "-----")
                    try {
                      val runnable = new Runnable {
                        override def run(): Unit = {
                          analysis(fs, inputFile, targetPath.getName + "_0", predictPoints, 2016, 288 * 3)
                        }
                      }
                      pools.execute(runnable)

                    }
                    catch {
                      case e: Exception => {
                        e.printStackTrace
                        println("This file is not handled:" + inputFile.getName + e.getMessage)
                      }
                    }
                  }
                }
              }
            }
          )
        }
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace
      }
    }
    SparkContextUtil.getInstance().stop()
  }

  @throws[Exception]
  private def createOutputWriters(fs: FileSystem, inputFilePath: Path, predictPoints: Array[Int], learnOutputWriters: Array[BufferedWriter], predictOutputWriters: Array[BufferedWriter]) {
    val inputFileNoExtension: String = inputFilePath.getParent.toString + "/" + inputFilePath.getName.substring(0, inputFilePath.getName.length - 4)
    for (i <- 0 until predictPoints.length) {
      val path = new Path(inputFileNoExtension + "_FT_" + predictPoints(i) + "_L.csv")
      if (!fs.exists(path)) {
        val os = fs.create(path, false)
        learnOutputWriters(i) = new BufferedWriter(new OutputStreamWriter(os))
      } else {
        val os = fs.append(path)
        learnOutputWriters(i) = new BufferedWriter(new OutputStreamWriter(os))
      }

    }
    for (i <- 0 until predictPoints.length) {
      val path = new Path(inputFileNoExtension + "_FT_" + predictPoints(i) + "_P.csv")
      if (!fs.exists(path)) {
        val os = fs.create(path, false)
        predictOutputWriters(i) = new BufferedWriter(new OutputStreamWriter(os))
      } else {
        val os = fs.append(path)
        predictOutputWriters(i) = new BufferedWriter(new OutputStreamWriter(os))
      }
    }
  }

  @throws[Exception]
  private def writeOutputTitleLine(learnOutputWriters: Array[BufferedWriter], predictOutputWriters: Array[BufferedWriter], predictPoints: Array[Int], inputTitleLine: String, targetName: String) {
    val cleanTargetName: String = targetName.substring(0, targetName.length - 2)
    for (i <- 0 until predictPoints.length) {
      val outputTitleLine: String = inputTitleLine + "," + cleanTargetName + "_" + predictPoints(i) + "_FT" + "," + cleanTargetName + "_" + predictPoints(i) + "\n"
      learnOutputWriters(i).write(outputTitleLine)
      predictOutputWriters(i).write(outputTitleLine)
    }
  }

  @throws[Exception]
  private def readLineDatas(inputReader: BufferedReader): util.ArrayList[String] = {
    val inputData: util.ArrayList[String] = new util.ArrayList[String](8192)
    val breaks = new Breaks
    breaks.breakable(
      while (true) {
        val inputLineData: String = inputReader.readLine
        if (inputLineData == null) {
          breaks.break()
        } else {
          inputData.add(inputLineData)
        }
      }
    )
    inputData
  }

  private def getTargetValues(inputLineDatas: util.List[String], targetColumnIndex: Int): util.List[Double] = {
    val targetValues: util.List[Double] = new util.ArrayList[Double](8192)
    import scala.collection.JavaConversions._
    for (inputLineData <- inputLineDatas) {
      val inputColumnDatas: Array[String] = inputLineData.split(",")
      val df = new DecimalFormat("0.00")
      targetValues.add(df.format(inputColumnDatas(targetColumnIndex).toDouble).toDouble)
    }
    targetValues
  }

  private def getFD(targetValues: util.List[Double], startIndex: Int, dftInterval: Int): Array[Complex] = {
    val td: Array[Double] = new Array[Double](dftInterval)
    for (i <- 0 until dftInterval) {
      td(i) = targetValues.get(i + startIndex)
    }
    DFT.dft(td)
  }

  @throws[Exception]
  private def writeOutputData(inputLineData: String, targetValues: util.List[Double], predictPoint: Int, inputLineIndex: Int, lastFD: Array[Complex], phase: Int, outputWriter: BufferedWriter) {
    val predictLineIndex: Int = predictPoint + inputLineIndex
    if (predictLineIndex < targetValues.size) {
      var predictValue: Double = DFT.idft_n(lastFD, phase + predictPoint).re
      val df = new DecimalFormat("0.00")
      predictValue=df.format(predictValue).toDouble
      val realValue: Double = targetValues.get(predictLineIndex)
      val outputLineData: String = inputLineData + "," + predictValue + "," + realValue + "\n"
      outputWriter.write(outputLineData)
    }
  }

  @throws[Exception]
  private def closeReaderAndWriters(inputReader: BufferedReader, learnOutputWriters: Array[BufferedWriter], predictOutputWriters: Array[BufferedWriter]) {
    inputReader.close
    for (i <- 0 until learnOutputWriters.length) {
      learnOutputWriters(i).flush
      learnOutputWriters(i).close
    }
    for (i <- 0 until predictOutputWriters.length) {
      predictOutputWriters(i).flush
      predictOutputWriters(i).close
    }
  }

  @throws[Exception]
  def analysis(fs: FileSystem, inputFilePath: Path, targetName: String, predictPoints: Array[Int], dftInterval: Int, predictDataLength: Int) {
    val path = inputFilePath
    val is = fs.open(path)
    val inputReader: BufferedReader = new BufferedReader(new InputStreamReader(is))
    val learnOutputWriters: Array[BufferedWriter] = new Array[BufferedWriter](predictPoints.length)
    val predictOutputWriters: Array[BufferedWriter] = new Array[BufferedWriter](predictPoints.length)
    createOutputWriters(fs: FileSystem, inputFilePath, predictPoints, learnOutputWriters, predictOutputWriters)
    val inputTitleLine: String = inputReader.readLine

//    println("-----inputTitleLine" + inputTitleLine + "-----")

    writeOutputTitleLine(learnOutputWriters, predictOutputWriters, predictPoints, inputTitleLine, targetName)
    var targetColumnIndex: Int = -1
    val inputColumnNames: Array[String] = inputTitleLine.split(",")
    val breaks = new Breaks
    breaks.breakable(
      for (i <- 0 until inputColumnNames.length) {
        if (inputColumnNames(i).toString.equalsIgnoreCase(targetName)) {
          targetColumnIndex = i
          breaks.break()
        }
      }

    )

    val inputLineDatas = readLineDatas(inputReader)
    val targetValues = getTargetValues(inputLineDatas, targetColumnIndex)
    var lastFD: Array[Complex] = null
    for (i <- 0 until inputLineDatas.size / dftInterval + 1) {
      val startIndex: Int = i * dftInterval
      val endIndex: Int = startIndex + dftInterval
      if (lastFD != null) {
        var phase: Int = 0
        while (phase < dftInterval && phase + startIndex < targetValues.size) {
          val inputLineIndex: Int = startIndex + phase
          val inputLineData: String = inputLineDatas.get(inputLineIndex)
          val lORp: Boolean = inputLineIndex + predictDataLength < targetValues.size
          for (k <- 0 until predictPoints.length) {
            val outputWriter: BufferedWriter = if (lORp) learnOutputWriters(k)
            else predictOutputWriters(k)
            writeOutputData(inputLineData, targetValues, predictPoints(k), inputLineIndex, lastFD, phase, outputWriter)
          }
          phase += 1
        }
      }
      if (endIndex < targetValues.size) lastFD = getFD(targetValues, i * dftInterval, dftInterval)
    }
    closeReaderAndWriters(inputReader, learnOutputWriters, predictOutputWriters)
  }


}
