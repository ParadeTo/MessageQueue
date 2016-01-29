package com.paradeto.log

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Ayou on 2015/11/23.
 */
object KafkaLog {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setAppName("Log").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
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
