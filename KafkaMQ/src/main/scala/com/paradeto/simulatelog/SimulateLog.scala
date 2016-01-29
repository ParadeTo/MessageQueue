package com.paradeto.simulatelog

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io._


/**
 * Created by Ayou on 2015/11/23.
 */
object SimulateLog {
  def index(length:Int) = {
    import java.util.Random
    val rdm = new Random

    rdm.nextInt(length)
  }
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <millisecond> <logFilename>")
      System.exit(1)
    }

    //数据来源
    val filename = args(0)
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    //写log
    new Thread() {
      override def run = {
        val out = new PrintWriter(args(2),"UTF-8")
        while (true) {
          Thread.sleep(args(1).toLong)
          val content = lines(index(filerow))
          println(content)
          out.write(content + '\n')
          out.flush()
        }
        out.close()
      }
    }.start()
  }
}
