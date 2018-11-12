package com.madhouse.dsp.Utils

import java.io.{File, PrintWriter}

import org.apache.commons.logging.{Log, LogFactory}

import scala.io.Source


/**
  * Created by Madhouse on 2017/8/28.
  */
object FileUtils {
  val log: Log = LogFactory.getLog(FileUtils.getClass)

  def subDir(dir: File): Iterator[File] = {
    val children = dir.listFiles().filter(_.isDirectory())
    children.toIterator ++ children.toIterator.flatMap(subDir)
  }

  def subFileDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subFileDir)
  }

  def outToFile(contant: String, file: String): Unit = {
    log.info(s"#####start to write into file: $file...")
    val out = new PrintWriter(file)
    out.println(contant)
    out.close()
    log.info(s"#####write to file end...")
  }

  def outToFile(contant: Array[String], file: String): Unit = {
    log.info(s"#####start to write into file: $file...")
    val out = new PrintWriter(file)
    for (c <- contant) {
      out.println(c)
    }
    out.close()
    log.info(s"#####write to file end...")
  }

  def inFromFile(path: String): Array[String] = {
    val source = Source.fromFile(path)
    val lines = source.getLines.toArray
    source.close
    log.info(s"#####read finishes. In file $path, there are ${lines.length} records....")
    lines
  }
}
