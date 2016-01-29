package com.paradeto.dao

import java.sql.{PreparedStatement, Connection}

import com.paradeto.entity.{Explorer, IP}
import org.slf4j.LoggerFactory

/**
 * 重复代码，不堪入目，懒得改了
 * Created by Ayou on 2015/11/9.
 */
class ExplorerDao {
  val logger = LoggerFactory.getLogger(this.getClass)

  def saveOrUpdate(conn:Connection,data:Explorer)={
    var sql = "select count(*) from explorer where explorer='"+data.explorer+"'";
    val ps : PreparedStatement = conn.prepareStatement(sql)
    val result = ps.executeQuery()
    result.next()
    if(result.getInt(1)>0){
      sql = "update explorer set explorer ='"+data.explorer+"',count="+data.count+" where explorer='"+data.explorer+"'";
      executeQuery(conn,sql);
    }else{
      sql = "insert into explorer (explorer,count) values (?,?);"
      executeQuery(conn,sql,data)
    }
  }

  def executeQuery(conn:Connection,sql:String,data:Explorer): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setString(1,data.explorer)
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
