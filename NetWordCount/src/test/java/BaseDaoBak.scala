//package com.paradeto.dao
//
//import org.apache.commons.dbutils.QueryRunner
//import org.apache.log4j.Logger
//
///**
// * Created by Ayou on 2015/11/9.
// */
//class BaseDao {
//  val run: QueryRunner = new QueryRunner()
//
//  val log: Logger = Logger.getLogger(classOf[MBaseDao])
//
//  /**
//   * 获取指定sql查询出所有集
//   * @param sql:查询sql语句，c:类类型 etc.classOf[T]
//   */
//  def findAll[T](sql: String, c: Class[T]): List[T] = {
//    val conn: Connection = DBConnPoolTool.getConnection("dev")
//    try {
//      val rs = run.query(conn, sql,
//        new BeanListHandler[T](c))
//      rs.toList
//    } catch {
//      case e: Exception => {
//        log.error("Query error:", e)
//        throw e
//      }
//    } finally {
//      DbUtils.close(conn)
//    }
//  }
//
//  def batchSave[T](sql: String, data: Array[Array[AnyRef]]): Unit = {
//    if (data.length > 0) {
//      val conn: Connection = DBConnPoolTool.getConnection("dev")
//      try {
//        conn.setAutoCommit(false)
//        run.batch(conn, sql, data)
//        conn.commit()
//        conn.setAutoCommit(true)
//      } catch {
//        case e: Exception => {
//          log.error("Batch save error:", e)
//          throw e
//        }
//      } finally {
//        DbUtils.close(conn)
//      }
//    }
//  }
//
//  def deleteAll(sql:String) : Unit = {
//    val conn: Connection = DBConnPoolTool.getConnection("dev")
//    var ps : PreparedStatement = null;
//    try {
//      ps  = conn.prepareStatement(sql);
//      ps.execute();
//    } catch {
//      case e: Exception => {
//        log.error("Batch save error:", e)
//        throw e
//      }
//    } finally {
//      DbUtils.close(conn, ps, null);
//    }
//  }
//}
