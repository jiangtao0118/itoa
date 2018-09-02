package com.wisdom.spark.etl.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by htgeng on 2017/4/13.
  */
object FSUtil extends Serializable{
  val hadoopConfiguration = new Configuration()
//  hadoopConfiguration.set("fs.defaultFS", "hdfs://ns1")
//  hadoopConfiguration.set("dfs.nameservices", "ns1")
//  hadoopConfiguration.set("dfs.ha.namenodes.ns1", "nn1,nn2")
//  hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn2","bd01:8022")
//  hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn1","bd02:8022")
//  hadoopConfiguration.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
//  System.setProperty("HADOOP_USER_NAME", "hdfs")
  val fs=FileSystem.newInstance(hadoopConfiguration)

}
