//package com.wisdom.spark.streaming.test
//
//import com.wisdom.spark.ml.mlUtil.ContextUtil
//import org.apache.spark.sql.hive.HiveContext
//
///**
//  * Created by htgeng on 2017/6/30.
//  */
//object TestHiveContext {
//  def main(args: Array[String]) {
//<<<<<<< .mine
//    //val hiveCtx1=ContextUtil.hiveCtx1
//    val hiveCtx1=ContextUtil.sqlContext
//
//    val run1=new Runnable {
//      override def run(): Unit = {
//        val table1 = hiveCtx1.sql("desc default.employee")
//        table1.show()
//      }
//    }
//
//    val run2=new Runnable {
//      override def run(): Unit = {
//        val table2 = hiveCtx1.sql("desc itm.system")
//        table2.show()
//      }
//    }
//
//    val thread1=new Thread(run1)
//    thread1.start()
//
//    val thread2=new Thread(run2)
//    thread2.start()
//||||||| .r4471
//    val hiveCtx1=ContextUtil.hiveCtx
//
//    val run1=new Runnable {
//      override def run(): Unit = {
//        val table1 = hiveCtx1.sql("desc default.employee")
//        table1.show()
//      }
//    }
//
//    val run2=new Runnable {
//      override def run(): Unit = {
//        val table2 = hiveCtx1.sql("desc itm.system")
//        table2.show()
//      }
//    }
//
//    val thread1=new Thread(run1)
//    thread1.start()
//
//    val thread2=new Thread(run2)
//    thread2.start()
//=======
////    val hiveCtx1=ContextUtil.hiveCtx
////
////    val run1=new Runnable {
////      override def run(): Unit = {
////        val table1 = hiveCtx1.sql("desc default.employee")
////        table1.show()
////      }
////    }
////
////    val run2=new Runnable {
////      override def run(): Unit = {
////        val table2 = hiveCtx1.sql("desc itm.system")
////        table2.show()
////      }
////    }
////
////    val thread1=new Thread(run1)
////    thread1.start()
////
////    val thread2=new Thread(run2)
////    thread2.start()
//>>>>>>> .r4565
////    hiveCtx1.setConf()
////    val table1 = hiveCtx1.sql("desc default.employee")
////    table1.show()
////    val hiveCtx2=new HiveContext(sc)
////    val table2 = hiveCtx1.sql("desc itm.system")
////    table2.show()
//
//  }
//
//}
