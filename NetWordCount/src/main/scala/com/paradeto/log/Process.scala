package com.paradeto.log

import com.paradeto.dao._
import com.paradeto.entity.{Explorer, PV, Area, IP}
import com.paradeto.util.IPSeeker
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by Ayou on 2015/11/23.
 */
object Process {
  // dao
  val ipDao = new IpDao
  val areaDao = new AreaDao
  val pvDao = new PvDao
  val explorerDao = new ExplorerDao
  // conn
  var conn = ConnectionPool.getConnection.getOrElse(null)
  // IPSeeker
  val ipSeeker = IPSeeker.getInstance()

  /**
   * 更新状态
   */
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def ip(inputDS: DStream[String]) = {
    inputDS.map(line => {
      val items = line.split(" ")
      println(line)
      // 只统计访问成功的
      if (items.length > 11&&items(8)=="200") (items(0).toString, 1)
      else ("Null", 1)
    }).updateStateByKey[Int](updateFunc).
      map(row => IP(row._1, row._2)).
      foreachRDD(rdd =>
      rdd.foreachPartition(part => {
        if (conn != null) {
          // 将结果保存到数据库
          part.foreach(record => ipDao.saveOrUpdate(conn,record))
        }
      }))
  }

  def area(inputDS: DStream[String]) = {
    inputDS.map(line => {
      val items = line.split(" ")
      if (items.length > 11&&items(8)=="200") (ipSeeker.getAddress(items(0)), 1)
      else ("Null", 1)
    }).updateStateByKey[Int](updateFunc).//foreachRDD(rdd=>{println("第"+i+"次");i = i+1;rdd.foreach(println)})
      map(row => Area(row._1, row._2)).
      foreachRDD(rdd =>
      rdd.foreachPartition(part => {
        if (conn != null) {
          //part.foreach(record => ipDao.executeQuery(conn, sql, record))
          part.foreach(record => areaDao.saveOrUpdate(conn,record))
        }
      }))
  }

  def pv(inputDS: DStream[String]) = {
    inputDS.map(line => {
      val items = line.split(" ")
      if (items.length > 11&&items(8)=="200") (items(10).replace("\"",""), 1)
      else ("Null", 1)
    }).updateStateByKey[Int](updateFunc).//foreachRDD(rdd=>{println("第"+i+"次");i = i+1;rdd.foreach(println)})
      map(row => PV(row._1, row._2)).
      foreachRDD(rdd =>
      rdd.foreachPartition(part => {
        if (conn != null) {
          //part.foreach(record => ipDao.executeQuery(conn, sql, record))
          part.foreach(record => pvDao.saveOrUpdate(conn,record))
        }
      }))
  }

  def explorer(inputDS: DStream[String]) = {
    inputDS.map(line => {
      val items = line.split(" ")
      if (items.length > 11&&items(8)=="200") (items(11).replace("\"",""), 1)
      else ("Null", 1)
    }).updateStateByKey[Int](updateFunc).//foreachRDD(rdd=>{println("第"+i+"次");i = i+1;rdd.foreach(println)})
      map(row => Explorer(row._1, row._2)).
      foreachRDD(rdd =>
      rdd.foreachPartition(part => {
        if (conn != null) {
          //part.foreach(record => ipDao.executeQuery(conn, sql, record))
          part.foreach(record => explorerDao.saveOrUpdate(conn,record))
        }
      }))
  }

  def closeConn=ConnectionPool.closeConnection(conn)
  //  def process(inputDS: DStream[String],flag:String){
  //    inputDS.map(line => {
  //      val items = line.split(" ")
  //      if (items.length > 11){
  //         flag match{
  //           case "pv"=>(items(8), 1)
  //           case "ip"=>(items(0), 1)
  //           case "area"=>(ipSeeker.getAddress(items(0)), 1)
  //        }
  //      } else ("Null", 1)
  //    }).updateStateByKey[Int](updateFunc).//foreachRDD(rdd=>{println("第"+i+"次");i = i+1;rdd.foreach(println)})
  //      map(row => PV(row._1, row._2)).
  //      foreachRDD(rdd =>
  //      rdd.foreachPartition(part => {
  //        if (conn != null) {
  //          //part.foreach(record => ipDao.executeQuery(conn, sql, record))
  //          part.foreach(record => pvDao.saveOrUpdate(conn,record))
  //        }
  //      }))
  //  }
}
