package com.paradeto.dao

import java.sql.{PreparedStatement, Connection}

import com.paradeto.entity.{Area, IP}
import org.slf4j.LoggerFactory

/**
 * 跟IpDao的大部分代码一样，这个设计肯定有问题
 * 暂时懒得弄了
 * Created by Ayou on 2015/11/9.
 */
class AreaDao {
  val logger = LoggerFactory.getLogger(this.getClass)

  def saveOrUpdate(conn:Connection,data:Area)={
    var sql = "select count(*) from area where area='"+data.area+"'";
    val ps : PreparedStatement = conn.prepareStatement(sql)
    val result = ps.executeQuery()
    result.next()
    if(result.getInt(1)>0){
      sql = "update area set area ='"+data.area+"',count="+data.count+" where area='"+data.area+"'";
      executeQuery(conn,sql);
    }else{
      sql = "insert into area (area,count) values (?,?);"
      executeQuery(conn,sql,data)
    }
  }

  def executeQuery(conn:Connection,sql:String,data:Area): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setString(1,data.area)
      ps.setInt(2,data.count)
      ps.executeUpdate()
    }catch{
      case exception:Exception=>
        logger.warn("Error in execution of query"+exception.printStackTrace())
    }
  }
  def executeQuery(conn:Connection,sql:String): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.executeUpdate()
    }catch{
      case exception:Exception=>
        logger.warn("Error in execution of query"+exception.printStackTrace())
    }
  }
}
