package com.paradeto.log

import com.paradeto.dao._
import com.paradeto.entity.{Area, IP,PV,Explorer}
import com.paradeto.util.IPSeeker
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 写了太多的重复代码
 * Created by Ayou on 2015/11/8.
 */
object Log extends Serializable {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Log").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    //分析统计
    Process.ip(lines)
    Process.area(lines)
    Process.pv(lines)
    Process.explorer(lines)
    ssc.start()
    ssc.awaitTermination()
    Process.closeConn
  }
}