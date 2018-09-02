package com.wisdom.spark.etl.ppn;

import com.wisdom.spark.etl.ppn.Complex;
import com.wisdom.spark.etl.ppn.DFT;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class PPNDataFTAnalysiser
{

//	public void analysis(String inputFilePath,String outputFilePath,int offset,int dftInterval)
//	{
//		//从输入文件加载 时间和cpu使用率信息
//		LinkedList<Long> timeStamps = new LinkedList<Long>();
//		LinkedList<Double> idleCPUs = new LinkedList<Double>();
//		try
//		{
//			File inputFile = new File(inputFilePath);
//			BufferedReader inputReader = new BufferedReader(new FileReader(inputFile),8192);
//
//			int inputLineIndex = -1;
//			while(true)
//			{
//				String inputLineData = inputReader.readLine();
//				inputLineIndex++;
//
//				if(inputLineData==null) break;
//				if(inputLineIndex < offset) continue;
//
//				String[] inputColumnDatas = inputLineData.split(",");
//
//				Calendar calendar = Calendar.getInstance();
//				int year = 2016;
//				int month = Integer.parseInt(inputColumnDatas[0]);
//				int date = Integer.parseInt(inputColumnDatas[1]);
//				int hourOfDay = Integer.parseInt(inputColumnDatas[2]);
//				int minute = Integer.parseInt(inputColumnDatas[3]);
//				calendar.set(year, month, date, hourOfDay, minute);
//				calendar.set(Calendar.SECOND,0);
//				calendar.set(Calendar.MILLISECOND,0);
//				long timeStamp = calendar.getTimeInMillis();
//
//				double idleCPU = Double.parseDouble(inputColumnDatas[7]);
//
//				while(true)
//				{
//					long lastTimeStamp = timeStamps.isEmpty()?Long.MAX_VALUE:timeStamps.getLast();
//
//					if(timeStamp - lastTimeStamp >  6 * 60 *1000)
//					{
//						long newTimeStamp = lastTimeStamp +  5 * 60 *1000;
//
//						timeStamps.addLast(newTimeStamp);
//						idleCPUs.addLast(idleCPU);
//					}
//					else
//					{
//						timeStamps.addLast(timeStamp);
//						idleCPUs.addLast(idleCPU);
//						break;
//					}
//				}
//			}
//
//			inputReader.close();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//
//
//		//do dft
//		double[] x = new double[dftInterval];
//		for(int i=0;i<dftInterval;i++)
//		{
//			x[i] = idleCPUs.get(i);
//		}
//		Complex[] y = DFT.dft(x);
//
//		//输出结果
//		try
//		{
//			File inputFile = new File(inputFilePath);
//			BufferedReader inputReader = new BufferedReader(new FileReader(inputFile),8192);
//
//			//打开输出数据文件，不存则新建，已存在则删除后新建
//			File outputFile = new File(outputFilePath);
//			if(outputFile.exists()) outputFile.delete();
//			outputFile.createNewFile();
//			BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile),8192);
//
//			int inputLineIndex = -1;
//			int timeStampsIndex = -1;
//
//			while(true)
//			{
//				String inputLineData = inputReader.readLine();
//				inputLineIndex++;
//
//				if(inputLineData==null) break;
//				if(inputLineIndex < offset) continue;
//
//				String[] inputColumnDatas = inputLineData.split(",");
//				Calendar calendar = Calendar.getInstance();
//				int year = 2016;
//				int month = Integer.parseInt(inputColumnDatas[0]);
//				int date = Integer.parseInt(inputColumnDatas[1]);
//				int hourOfDay = Integer.parseInt(inputColumnDatas[2]);
//				int minute = Integer.parseInt(inputColumnDatas[3]);
//				calendar.set(year, month, date, hourOfDay, minute);
//				calendar.set(Calendar.SECOND,0);
//				calendar.set(Calendar.MILLISECOND,0);
//				long timeStamp = calendar.getTimeInMillis();
//
//				double idleCPUs5m = 0;
//				double idleCPUs10m = 0;
//				double idleCPUs15m = 0;
//				double idleCPUs20m = 0;
//				double idleCPUs25m = 0;
//				while(true)
//				{
//					timeStampsIndex ++;
//					if(timeStamps.get(timeStampsIndex) == timeStamp)
//					{
//						idleCPUs5m = DFT.idft_n(y, timeStampsIndex+1).re();
//						idleCPUs10m = DFT.idft_n(y, timeStampsIndex+2).re();
//						idleCPUs15m = DFT.idft_n(y, timeStampsIndex+3).re();
//						idleCPUs20m = DFT.idft_n(y, timeStampsIndex+4).re();
//						idleCPUs25m = DFT.idft_n(y, timeStampsIndex+5).re();
//						break;
//					}
//				}
//
//				String outputLineData = idleCPUs5m+","+idleCPUs10m+","+idleCPUs15m+","+idleCPUs20m+","+idleCPUs25m+","+inputLineData+"\n";
//				//String outputLineData = idleCPUs15m+","+inputLineData+"\n";
//				outputWriter.write(outputLineData);
//			}
//
//			inputReader.close();
//
//			outputWriter.flush();
//			outputWriter.close();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
    /**
     * 创建输出文件，包括学习数据文件和预测数据文件
     * @param inputFilePath：输入文件的路径
     * @param predictPoints：预测之后几个点
     * @param learnOutputWriters：学习数据文件的BufferedWriter
     * @param predictOutputWriters：预测数据文件的BufferedWriter
     */
    private void createOutputWriters(String inputFilePath,int[] predictPoints,BufferedWriter[] learnOutputWriters,BufferedWriter[] predictOutputWriters) throws Exception
    {
        String inputFileNoExtension = inputFilePath.substring(0,inputFilePath.length()-4);

        for(int i=0;i<predictPoints.length;i++)
        {

            File learnOutputFile = new File(inputFileNoExtension+"_FT_"+predictPoints[i]+"_L.csv");
            if(learnOutputFile.exists()) learnOutputFile.delete();
            learnOutputFile.createNewFile();
            learnOutputWriters[i] = new BufferedWriter(new FileWriter(learnOutputFile));
        }

        for(int i=0;i<predictPoints.length;i++)
        {

            File predictOutputFile = new File(inputFileNoExtension+"_FT_"+predictPoints[i]+"_P.csv");
            if(predictOutputFile.exists()) predictOutputFile.delete();
            predictOutputFile.createNewFile();
            predictOutputWriters[i] = new BufferedWriter(new FileWriter(predictOutputFile));
        }
    }
    /**
     * 为输出文件写表头
     * @param learnOutputWriters：学习数据文件的BufferedWriter
     * @param predictOutputWriters：预测数据文件的BufferedWriter
     * @param predictPoints：预测之后几个点
     * @param inputTitleLine：输入文件的表头
     * @param targetName：要做FT变换的列的名称
     */
    private void writeOutputTitleLine(BufferedWriter[] learnOutputWriters,BufferedWriter[] predictOutputWriters,int[] predictPoints,String inputTitleLine,String targetName) throws Exception
    {
        String cleanTargetName = targetName.substring(0,targetName.length()-2);
        for(int i=0;i<predictPoints.length;i++)
        {
            String outputTitleLine = inputTitleLine + "," + cleanTargetName + "_"+predictPoints[i]+"_FT" +"," + cleanTargetName + "_"+predictPoints[i]+"\n";
            learnOutputWriters[i].write(outputTitleLine);
            predictOutputWriters[i].write(outputTitleLine);
        }
    }
    /**
     * 读一行数据
     * @param inputReader：输入文件的BufferedReader
     * @return ：这行数据的内容
     */
    private List<String> readLineDatas(BufferedReader inputReader) throws Exception
    {
        List<String> inputData = new ArrayList<String>(8192);
        while(true)
        {
            String inputLineData = inputReader.readLine();
            if(inputLineData==null)
            {
                break;
            }
            else
            {
                inputData.add(inputLineData);
            }
        }
        return inputData;
    }
    /**
     * 获得指定列的所有数据
     * @param inputLineDatas：输入文件所有的行
     * @param targetColumnIndex：指定的列
     * @return ：指定列的所有数据
     */
    private List<Double> getTargetValues(List<String> inputLineDatas,int targetColumnIndex)
    {
        List<Double> targetValues = new ArrayList<Double>(8192);
        for(String inputLineData:inputLineDatas)
        {
            String[] inputColumnDatas = inputLineData.split(",");
            targetValues.add(Double.parseDouble(inputColumnDatas[targetColumnIndex]));
        }
        return targetValues;
    }
    /**
     * 对指定的数据进行DFT
     * @param targetValues：指定列的所有数据
     * @param startIndex：开始的行数
     * @param dftInterval：做DFT的数据的长度
     */
    private Complex[] getFD(List<Double> targetValues, int startIndex, int dftInterval)
    {
        double[] td = new double[dftInterval];
        for(int i=0;i<dftInterval;i++)
        {
            td[i] = targetValues.get(i+startIndex);
        }
        return DFT.dft(td);
    }
    /**
     * 写输出文件（学习数据文件和预测数据文件）
     * @param inputLineData：输入文件的一行
     * @param targetValues：做FT变换的原数据
     * @param predictPoint：预测的点的偏移量
     * @param inputLineIndex：输入文件的行号
     * @param lastFD：上一个周期的模型
     * @param phase：当前行相对于上一个周期结束点的行数
     * @param outputWriter：输出文件的BufferedWriter
     */
    private void writeOutputData(String inputLineData,List<Double> targetValues,int predictPoint,int inputLineIndex,Complex[] lastFD,int phase,BufferedWriter outputWriter) throws Exception
    {
        int predictLineIndex = predictPoint + inputLineIndex;
        if(predictLineIndex <targetValues.size())
        {
            double predictValue = DFT.idft_n(lastFD, phase+predictPoint).re();
            double realValue = targetValues.get(predictLineIndex);
            String outputLineData = inputLineData + ","+predictValue+ "," + realValue+"\n";
            outputWriter.write(outputLineData);
        }
    }
    /**
     * 关闭读写文件句柄
     * @param inputReader：输入文件BufferedReader
     * @param learnOutputWriters：学习数据文件BufferedWriter
     * @param predictOutputWriters：验证数据文件BufferedWriter
     */
    private void closeReaderAndWriters(BufferedReader inputReader,BufferedWriter[] learnOutputWriters,BufferedWriter[] predictOutputWriters) throws Exception
    {
        inputReader.close();

        for(int i=0;i<learnOutputWriters.length;i++)
        {
            learnOutputWriters[i].flush();
            learnOutputWriters[i].close();
        }

        for(int i=0;i<predictOutputWriters.length;i++)
        {
            predictOutputWriters[i].flush();
            predictOutputWriters[i].close();
        }
    }

    /**
     * 读入csv文件，找到指定的列，对指定的列做 FT变换，将预测结果加入原文件中，然后分为学习数据和验证数据分别写入两个新文件中
     * @param inputFilePath ： csv文件路径
     * @param targetName：做FT变换的列的名称
     * @param predictPoints：预测之后几个点
     * @param dftInterval：做DFT的数据分段的长度
     * @param predictDataLength：验证文件的行数
     */
    public void analysis(String inputFilePath,String targetName,int[] predictPoints,int dftInterval,int predictDataLength) throws Exception
    {
        File inputFile = new File(inputFilePath);
        BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));

        BufferedWriter[] learnOutputWriters = new BufferedWriter[predictPoints.length];
        BufferedWriter[] predictOutputWriters = new BufferedWriter[predictPoints.length];
        createOutputWriters(inputFilePath,predictPoints,learnOutputWriters,predictOutputWriters);

        String inputTitleLine = inputReader.readLine();

        writeOutputTitleLine(learnOutputWriters,predictOutputWriters,predictPoints,inputTitleLine,targetName);

        int targetColumnIndex = -1;
        String[] inputColumnNames = inputTitleLine.split(",");
        for(int i=0;i<inputColumnNames.length;i++) if(inputColumnNames[i].equals(targetName)) targetColumnIndex = i;

        List<String> inputLineDatas = readLineDatas(inputReader);
        List<Double> targetValues = getTargetValues(inputLineDatas,targetColumnIndex);

        Complex[] lastFD = null;
        for(int i=0;i<inputLineDatas.size()/dftInterval+1;i++)
        {
            int startIndex = i*dftInterval;
            int endIndex = startIndex + dftInterval;

            if(lastFD != null)
            {
                for(int phase=0;phase<dftInterval && phase+startIndex<targetValues.size();phase++)
                {
                    int inputLineIndex = startIndex + phase;
                    String inputLineData = inputLineDatas.get(inputLineIndex);
                    boolean lORp = inputLineIndex + predictDataLength < targetValues.size();

                    for(int k=0;k<predictPoints.length;k++)
                    {
                        BufferedWriter outputWriter = lORp ? learnOutputWriters[k]:predictOutputWriters[k];
                        writeOutputData(inputLineData,targetValues,predictPoints[k],inputLineIndex,lastFD,phase,outputWriter);
                    }
                }
            }

            if(endIndex < targetValues.size()) lastFD = getFD(targetValues,i*dftInterval,dftInterval);
        }

        closeReaderAndWriters(inputReader,learnOutputWriters,predictOutputWriters);
    }

    public static void main(String[] args)
    {
        String basePath = args[0];
        String predictType=args[1];

        PPNDataFTAnalysiser analysiser = new PPNDataFTAnalysiser();
        //analysiser.analysis("resource/ASCECUP01_p3.csv","resource/ASCECUP01_p3_FT.csv",1,8642);
        try
        {
            File baseFile = new File(basePath);
            File[] hostFolders = baseFile.listFiles();
            for(int i=0;i<hostFolders.length;i++)
            {
                File hostFolder = hostFolders[i];
                if(hostFolder.isDirectory())
                {
                    File[] targetNameFolders = hostFolder.listFiles();
                    for(int j=0;j<targetNameFolders.length;j++)
                    {
                        File targetNameFolder = targetNameFolders[j];
                        if(targetNameFolder.isDirectory())
                        {
                            File[] inputFiles = targetNameFolder.listFiles();
                            boolean flag=false;
                            for(int k=0;k<inputFiles.length;k++)
                            {
                                File inputFile = inputFiles[k];
                                if(inputFile.isFile()&&inputFile.getName().contains("FT"))
                                {
                                    flag=true;
                                    break;
                                }
                            }

                            if(flag){
                                continue;
                            }



                            for(int k=0;k<inputFiles.length;k++)
                            {
                                File inputFile = inputFiles[k];
                                if(inputFile.isFile()&&!inputFile.getName().contains("FT"))
                                {
                                    int[] predictPoints={1,3,6,12};
                                    if(predictType.equalsIgnoreCase("5")) {
                                        int[] temp={1,3,6,12};
                                        predictPoints= temp;
                                    } else if(predictType.equalsIgnoreCase("1")) {
                                        int[] temp={5,15,30,60};
                                        predictPoints = temp;
                                    }

                                    try{
                                        analysiser.analysis(inputFile.getPath(),targetNameFolder.getName()+"_0",predictPoints,2016,288*3);

                                    }catch(Exception e){
                                        e.printStackTrace();
                                        System.out.println("This file is not handled:"+inputFile.getPath()+e.getMessage());
                                    }
                                }
                            }
                        }
                    }

                }
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }



}