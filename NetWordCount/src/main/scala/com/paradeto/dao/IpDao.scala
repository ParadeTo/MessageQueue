package com.paradeto.dao

import java.sql.{PreparedStatement, Connection}

import com.paradeto.entity.IP
import org.slf4j.LoggerFactory

/**
 * Created by Ayou on 2015/11/9.
 */
class IpDao {
  val logger = LoggerFactory.getLogger(this.getClass)

  def saveOrUpdate(conn:Connection,data:IP)={
    var sql = "select count(*) from ip where ip='"+data.ip+"'";
    val ps : PreparedStatement = conn.prepareStatement(sql)
    val result = ps.executeQuery()
    result.next()
    if(result.getInt(1)>0){
      sql = "update ip set ip ='"+data.ip+"',count="+data.count+" where ip='"+data.ip+"'";
      executeQuery(conn,sql);
    }else{
      sql = "insert into ip (ip,count) values (?,?);"
      executeQuery(conn,sql,data)
    }
  }

  def executeQuery(conn:Connection,sql:String,data:IP): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setString(1,data.ip)
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
