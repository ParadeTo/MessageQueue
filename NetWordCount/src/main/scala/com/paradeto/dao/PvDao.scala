package com.paradeto.dao

import java.sql.{PreparedStatement, Connection}

import com.paradeto.entity.{PV, IP}
import org.slf4j.LoggerFactory

/**
 * Created by Ayou on 2015/11/9.
 */
class PvDao {
  val logger = LoggerFactory.getLogger(this.getClass)

  def saveOrUpdate(conn:Connection,data:PV)={
    var sql = "select count(*) from pv where page='"+data.page+"'";
    val ps : PreparedStatement = conn.prepareStatement(sql)
    val result = ps.executeQuery()
    result.next()
    if(result.getInt(1)>0){
      sql = "update pv set page ='"+data.page+"',count="+data.count+" where page='"+data.page+"'";
      executeQuery(conn,sql);
    }else{
      sql = "insert into pv (page,count) values (?,?);"
      executeQuery(conn,sql,data)
    }
  }

  def executeQuery(conn:Connection,sql:String,data:PV): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setString(1,data.page)
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
