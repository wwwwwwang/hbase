package com.madhouse.dsp

import java.io.File
import java.util.concurrent.TimeUnit

import com.madhouse.dsp.Utils.Configer._
import com.madhouse.dsp.Utils.FileUtils._
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import net.minidev.json.JSONObject
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.log4j.BasicConfigurator

/**
  * Created by Madhouse on 2017/8/28.
  */
object HbaseOptionTest {
  val log: Log = LogFactory.getLog(HbaseOption.getClass)
  BasicConfigurator.configure()

  def getConfiguration: Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf
  }

  def getHbaseConn(conf: Configuration): HBaseAdmin = {
    val admin = new HBaseAdmin(conf)
    admin
  }

  def releaseConn(admin: HBaseAdmin): Unit = {
    try {
      if (admin != null) {
        admin.close()
      }
    } catch {
      case ex: Exception => ex.getMessage
    }
  }

  def getHTable(conf: Configuration, tableName: String): HTable = {
    val table: HTable = new HTable(conf, tableName)
    table
  }

  def closeHTable(table: HTable): Unit = {
    if (table != null) {
      table.close()
    }
  }

  def isExist(admin: HBaseAdmin, tableName: String): Boolean = {
    val res = admin.tableExists(tableName)
    res
  }

  def createTable(admin: HBaseAdmin, tableName: String, columnFamilys: Array[String]): Unit = {
    if (admin.tableExists(tableName)) {
      println(s"表:$tableName 已经存在!")
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      admin.createTable(tableDesc)
      println(s"表:$tableName 创建成功")
    }
  }

  def deleteTable(admin: HBaseAdmin, tableName: String): Unit = {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("删除表成功!")
    } else {
      println("表" + tableName + " 不存在")
    }
  }

  def addRow(table: HTable, row: String, columnFaily: String, column: String, value: String): Unit = {
    val put: Put = new Put(Bytes.toBytes(row))
    put.add(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }

  def delRow(table: HTable, row: String): Unit = {
    val delete: Delete = new Delete(Bytes.toBytes(row))
    table.delete(delete)
  }

  def delMultiRows(table: HTable, rows: Array[String]): Unit = {
    val deleteList = for (row <- rows) yield new Delete(Bytes.toBytes(row))
    table.delete(deleteList.toSeq.asJava)
  }

  def getRow(table: HTable, row: String): Int = {
    val get: Get = new Get(Bytes.toBytes(row))
    val result: Result = table.get(get)
    for (rowKv <- result.raw()) {
      println(new String(rowKv.getFamily))
      println(new String(rowKv.getQualifier))
      println(rowKv.getTimestamp)
      println(new String(rowKv.getRow))
      println(new String(rowKv.getValue))
    }
    val cnt = result.getRow.length
    cnt
  }

  def getCustomered(table: HTable, row: String, filter: List[String]): Boolean = {
    val get: Get = new Get(Bytes.toBytes(row))
    val result: Result = table.get(get)
    var isHit = false
    val tags = new ArrayBuffer[String]()
    for (rowKv <- result.raw()) {
      /*println(new String(rowKv.getFamily))
      println(new String(rowKv.getQualifier))
      println(rowKv.getTimestamp)
      println(new String(rowKv.getRow))
      println(new String(rowKv.getValue))*/
      val tag = new String(rowKv.getQualifier).substring(3)
      /*if (filter.contains(tag)) {
        log.info(s"for row: $row, the hit tag is $tag.")
        isHit = true
      }*/
      tags += tag
    }
    if (tags.toList.intersect(filter).nonEmpty) {
      isHit = true
    }
    isHit
  }

  def getAllRows(table: HTable): Boolean = {
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    val isHas = it.hasNext
    while (it.hasNext) {
      val next: Result = it.next()
      for (kv <- next.raw()) {
        println(new String(kv.getRow))
        println(new String(kv.getFamily))
        println(new String(kv.getQualifier))
        println(new String(kv.getValue))
        println(kv.getTimestamp)
        println("---------------------")
      }
    }
    isHas
  }

  def map2Json(map: mutable.Map[String, Any]): String = {
    val jsonString = JSONObject.toJSONString(map)
    jsonString
  }

  def scanTable2Json(table: HTable, outPath: String): Int = {
    val res = mutable.Buffer[String]()
    var cnt = 0
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    val isHas = it.hasNext
    while (it.hasNext) {
      val map = mutable.Map[String, Any]()
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      val rowkey = row.substring(0, row.indexOf(":"))
      if (rowkey.length == 32 || rowkey.length == 36) {
        map += ("rowkey" -> rowkey)
        var value = ""
        for (kv <- next.raw()) {
          //map += (new String(kv.getFamily) + ":" + new String(kv.getQualifier) -> new String(kv.getValue))
          value += new String(kv.getQualifier) + ","
        }
        cnt += 1
        map += ("value" -> value.substring(0, value.lastIndexOf(",")))
        val jsonString = map2Json(map)
        res += jsonString
      }
      if (cnt % 100000 == 0 && cnt > 0) {
        outToFile(res.toArray, outPath + cnt / 100000)
        res.clear()
        Thread.sleep(1000)
      }
    }
    outToFile(res.toArray, outPath + cnt / 100000)
    cnt
  }

  def scanTable2JsonArray(table: HTable, outPath: String): Int = {
    val res = mutable.Buffer[String]()
    var cnt = 0
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    val isHas = it.hasNext
    while (it.hasNext) {
      val map = mutable.Map[String, Any]()
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      val rowkey = row.substring(0, row.indexOf(":"))
      if (rowkey.length == 32 || rowkey.length == 36) {
        map += ("did" -> rowkey)
        val value = mutable.ArrayBuffer[String]()
        for (kv <- next.raw()) {
          //map += (new String(kv.getFamily) + ":" + new String(kv.getQualifier) -> new String(kv.getValue))
          value += new String(kv.getQualifier)
        }
        cnt += 1
        map += ("tags" -> value)
        val jsonString = map2Json(map)
        res += jsonString
      }
      if (cnt % 100000 == 0 && cnt > 0) {
        outToFile(res.toArray, outPath + cnt / 100000)
        res.clear()
        Thread.sleep(1000)
      }
    }
    outToFile(res.toArray, outPath + cnt / 100000)
    cnt
  }

  def process(table: HTable, dir: String, filter: List[String]): Unit = {
    val files = subFileDir(new File(dir))
    for (f <- files) {
      val inpath = f.toString
      val outpath = inpath.replaceFirst("\\..*", "_proceed.csv")
      val suffix = if (inpath.contains("and")) "didmd5" else "ifa"
      val lines = inFromFile(inpath)
      var cnt = 0
      val filterRowkey = lines.filter(line => {
        cnt += 1
        if (cnt % 1000 == 0) {
          Thread.sleep(1000)
        }
        val rowkey = line + ":" + suffix
        getCustomered(table, rowkey.toLowerCase, filter)
      })
      /*lines.foreach(line=>{
        val rowkey = line+":"+suffix
        if(getCustomered(table, rowkey, filter)){

        }
      })*/
      outToFile(filterRowkey, outpath)
    }
  }

  def main(args: Array[String]): Unit = {

    var rowkeyPath = ""
    var filterTagPath = ""
    var table = tableName
    var json = false

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("j", "json", false, "export data of hbase as json file")
    opt.addOption("n", "name", true, "the table name is used in hbase")
    opt.addOption("r", "rowkey", true, "the path of file contains the rowkey will be got from hbase")
    opt.addOption("t", "tag", true, "the path of file contains the tags used for filtering")

    val formatstr = "sh run.sh yarn-cluster|yarn-client|local ...."
    val formatter = new HelpFormatter
    val parser = new PosixParser

    var cl: CommandLine = null
    try
      cl = parser.parse(opt, args)

    catch {
      case e: ParseException =>
        e.printStackTrace()
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }

    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("j")) json = true
    if (cl.hasOption("n")) table = cl.getOptionValue("n")
    if (cl.hasOption("r")) rowkeyPath = cl.getOptionValue("r")
    if (cl.hasOption("t")) filterTagPath = cl.getOptionValue("t")

    log.info(s"#####rowkeyPath = $rowkeyPath, filterTagPath = $filterTagPath, tableName = $table, outjson = $json")


    val conf = getConfiguration
    val hTable = getHTable(conf, table)

    if (json) {
      val count = scanTable2JsonArray(hTable, "jsonOfHbase")
      log.info(s"#####finish...there are $count records in table $tableName")
    } else {
      val tags = inFromFile(filterTagPath)
      val tagsArray = tags(0).split(",").toList
      if (tagsArray.isEmpty) {
        log.info("#####Wrong: the filter tags are empty, exit....")
        System.exit(1)
      }
      process(hTable, rowkeyPath, tagsArray)
    }


    log.info("#####finished....")
  }
}
