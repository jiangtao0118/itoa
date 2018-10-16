package com.wisdom.test;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class testColumn {

    public  static void main(String[] args){
        String path = "data.csv";
        HashMap<String,String> map = new HashMap<String, String>();
        map.put("header","ture");
        map.put("seq",",");
        map.put("path",path);
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("spark session example").getOrCreate();
        DataFrame data =spark.read().options(map).format("csv").load();

        data.show();
    }
}
