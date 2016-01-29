package com.paradeto.dao

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
 * Created by Ayou on 2015/11/9.
 */
object ConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)
  private val connectionPool = {
    try{
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/log")
      config.setUsername("root")
      config.setPassword("111111")
      config.setMinConnectionsPerPartition(2)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(3)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(true)
      Some(new BoneCP(config))
    } catch {
      case exception:Exception=>
        logger.warn("Error in creation of connection pool"+exception.printStackTrace())
        None
    }
  }

  def getConnection:Option[Connection] ={
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) connection.close()
  }
}
