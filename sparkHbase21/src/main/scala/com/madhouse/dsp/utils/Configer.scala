package com.madhouse.dsp.utils

import java.io.File
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object Configer {
  implicit private var config: Config = _

  val defaultHdfsPath = "/madplatform/config/"
  var path: String = "application.conf"
  var rootName: String = "app"

  def inputStream2String(is: FSDataInputStream): String = {
    scala.io.Source.fromInputStream(is).getLines().mkString("\n")
  }

  def init(configName: String, rootName: String) {
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
    //println(s"#####directory.getAbsolutePath = $filePath")
    val localPath = filePath.substring(0, filePath.lastIndexOf("/") + 1) + configName
    println(s"#####path = $localPath")
    val configFile = new File(localPath)
    if (configFile.exists()) {
      config = ConfigFactory.parseFile(configFile).getConfig(rootName)
    } else {
      println(s"####Property file not found:$localPath, try to get it from hdfs...")

      val hdfsPath = defaultHdfsPath + "/" + configName
      println(s"#####start to read config($hdfsPath) file from hdfs")
      val conf: Configuration = new Configuration
      conf.setBoolean("fs.hdfs.impl.disable.cache", true)
      val fs = FileSystem.get(URI.create(hdfsPath), conf)
      if (fs.exists(new Path(hdfsPath))) {
        val in = fs.open(new Path(hdfsPath))
        /*val str = inputStream2String(in)
        println(s"#####string = $str")*/
        config = ConfigFactory.parseString(inputStream2String(in)).getConfig(rootName)
        in.close()
        fs.close()
      } else {
        println(s"####$hdfsPath in hdfs is not exist, cannot get config and exit...")
        fs.close()
        sys.exit(1)
      }
    }
  }

  def getWithElse[T](path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      defaultValue match {
        case _: Int => config.getInt(path).asInstanceOf[T]
        case _: String => config.getString(path).asInstanceOf[T]
        case _: Double => config.getDouble(path).asInstanceOf[T]
        case _: Long => config.getLong(path).asInstanceOf[T]
        case _: Boolean => config.getBoolean(path).asInstanceOf[T]
        case _ => defaultValue
      }
    } else {
      defaultValue
    }
  }

  val configDefault = init(path,rootName)

  val sparkAppName: String = getWithElse("spark.app_name", "Hbase2Hdfs")
  val zookeeperPort: String = getWithElse("hbase.zookeeper.clientport", "2181")
  //val zookeeperQuorum: String = getOrElse("hbase.zookeeper.quorum","10.10.35.12,10.10.35.13,10.10.35.14")
  val zookeeperQuorum: String = getWithElse("hbase.zookeeper.quorum","10.10.66.55,10.10.66.56,10.10.66.57")
  val tableName: String = getWithElse("hbase.table.name", "maddsp_multiplefusion_data")
  val pathOfLog: String = getWithElse("out.basePath", "/tmp/whsh/projects2hbase")
  val basePathOfLog: String = if(pathOfLog.endsWith("/")) pathOfLog.dropRight(1) else pathOfLog
  }