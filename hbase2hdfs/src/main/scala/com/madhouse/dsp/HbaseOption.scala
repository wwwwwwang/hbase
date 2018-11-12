package com.madhouse.dsp

import java.io.File

import com.madhouse.dsp.Utils.Configer._
import com.madhouse.dsp.Utils.FileUtils._
import net.minidev.json.JSONObject
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor}

import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/8/28.
  */
object HbaseOption {
  //val log: Log = LogFactory.getLog(HbaseOption.getClass)
  //BasicConfigurator.configure()

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

  def delRowWithColumn(table: HTable, row: String, family: String, column: String): Unit = {
    val delete: Delete = new Delete(Bytes.toBytes(row))
    delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(column))
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
        println(s"for row: $row, the hit tag is $tag.")
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

  def scanTable4Timestamp(table: HTable, outPath: String): Unit = {
    val numberOfFile = 100000
    val res = mutable.Buffer[String]()
    var cnt = 1
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      if (row.contains(":")) {
        val rowkey = row.substring(0, row.indexOf(":"))
        if (rowkey.length == 32 || rowkey.length == 36) {
          var maxTimeStamp = 0L
          for (kv <- next.raw()) {
            //map += (new String(kv.getFamily) + ":" + new String(kv.getQualifier) -> new String(kv.getValue))
            val t = kv.getTimestamp
            if (t > maxTimeStamp) {
              maxTimeStamp = t
            }
          }
          res += s"$rowkey,$maxTimeStamp"
        }
      }
      if (res.length >= numberOfFile) {
        outToFile(res.toArray, s"${outPath}_4timestamp_$cnt")
        println(s"######get ${cnt * numberOfFile} records from hbase")
        res.clear()
        cnt += 1
        Thread.sleep(1000)
      }
    }
    outToFile(res.toArray, s"${outPath}_4timestamp_$cnt")
    println(s"######there are ${(cnt - 1) * numberOfFile + res.length} records in hbase")
  }

  def scanTableWithColumnPrefix(table: HTable, prefix: String, outPath: String): Unit = {
    val numberOfFile = 100000
    val res = mutable.Buffer[String]()
    var cnt = 1
    val scan = new Scan()
    val filter = new ColumnPrefixFilter(Bytes.toBytes(prefix)); // 前缀为 my-prefix
    scan.setFilter(filter)
    val results: ResultScanner = table.getScanner(scan)
    val it = results.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      if (row.contains(":")) {
        val rowkey = row.substring(0, row.indexOf(":"))
        if (rowkey.length == 32 || rowkey.length == 36) {
          res += s"$rowkey"
        }
      }
      if (res.length >= numberOfFile) {
        outToFile(res.toArray, s"${outPath}_columnPrefix_$cnt")
        println(s"######get ${cnt * numberOfFile} records from hbase")
        res.clear()
        cnt += 1
        Thread.sleep(1000)
      }
    }
    outToFile(res.toArray, s"${outPath}_columnPrefix_$cnt")
    println(s"######there are ${(cnt - 1) * numberOfFile + res.length} records with columnprefix $prefix in hbase")
  }

  def scanTable4Dirty(table: HTable, outPath: String): Unit = {
    val rowkeys = mutable.Buffer[String]()
    val columns = mutable.Buffer[String]()
    var cntRowkey = 1
    var cntColumn = 1
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      if (row.contains(":")) {
        val rowkey = row.substring(0, row.indexOf(":"))
        if (rowkey.length == 32 || rowkey.length == 36) {
          for (kv <- next.raw()) {
            //map += (new String(kv.getFamily) + ":" + new String(kv.getQualifier) -> new String(kv.getValue))
            //value += new String(kv.getQualifier) + ","
            val c = new String(kv.getQualifier)
            if (c.toLowerCase.contains("_ap_")) {
              columns += s"$rowkey,$c"
            }
          }
        } else {
          rowkeys += row
        }
      } else {
        rowkeys += row
      }
      if (columns.length % 100000 == 0 && columns.nonEmpty) {
        outToFile(columns.toArray, s"${outPath}_columns_$cntColumn")
        println(s"#####now having found ${cntColumn * 100000} dirty columns in hbase....")
        columns.clear()
        cntColumn += 1
        Thread.sleep(1000)
      }
      if (rowkeys.length % 100000 == 0 && rowkeys.nonEmpty) {
        outToFile(rowkeys.toArray, s"${outPath}_rowkeys_$cntRowkey")
        println(s"#####now having found ${cntColumn * 100000} dirty rowkey in hbase....")
        rowkeys.clear()
        cntRowkey += 1
        Thread.sleep(1000)
      }
    }
    outToFile(columns.toArray, s"${outPath}_columns_${columns.length / 100000}")
    outToFile(rowkeys.toArray, s"${outPath}_rowkeys_${rowkeys.length / 100000}")
  }

  def hasNoUpper(str: String): Boolean = {
    val s = str.toLowerCase()
    s.equals(str)
  }

  def isLegalColumn(str: String): Boolean = {
    val a = str.replaceAll("[0-9]*", "").replaceAll("_", "")
    a.length == 2
  }

  def insertAt(str: String, i: Int = 6, sstr: String = "0"): String = {
    s"${str.substring(0, i)}$sstr${str.substring(i)}"
  }

  def replace(str: String): String = {
    if (str.startsWith("mh_")) {
      val s = str.replaceFirst("mh_", "010")
      insertAt(s)
    }
    else if (str.startsWith("td_"))
      str.replaceFirst("td_", "011")
    else if (str.startsWith("up_"))
      str.replaceFirst("up_", "012")
    else if (str.startsWith("ap_"))
      str.replaceFirst("ap_", "015")
    else if (str.startsWith("kd_"))
      str.replaceFirst("kd_", "017")
    else if (str.startsWith("am_"))
      str.replaceFirst("am_", "018")
    else if (str.startsWith("qx_"))
      str.replaceFirst("qx_", "019")
    else if (str.startsWith("sz_"))
      str.replaceFirst("sz_", "021")
    else
      str
  }

  def scanTable(table: HTable, outPath: String, check: Boolean): Unit = {
    val numberOfFile = 100000
    val res = mutable.Buffer[String]()
    val dirtyRowkey = mutable.Buffer[String]()
    val dirtyColumn = mutable.Buffer[String]()
    var cnt = 1
    var dirtyRowkeyCnt = 1
    var dirtyColumnCnt = 1
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    while (it.hasNext) {
      val map = mutable.Map[String, Any]()
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      if ((row.length == 39 || row.length == 40) && row.contains(":") && hasNoUpper(row)) {
        //jiben hefa de rowkey
        val rowkey = row.substring(0, row.indexOf(":"))
        if (rowkey.length == 32 || rowkey.length == 36) {
          map += ("did" -> (if (rowkey.contains("-")) rowkey.toUpperCase + ":ifa" else rowkey + ":didmd5"))
          val os = if (rowkey.contains("-")) 1 else 0
          var value = ""
          for (kv <- next.raw()) {
            val col = new String(kv.getQualifier)
            if (isLegalColumn(col)) {
              value += replace(col) + ","
            } else {
              if (check) {
                dirtyColumn += s"$rowkey,$col"
              }
            }
          }
          map += ("tags" -> value.dropRight(1))
          map += ("os" -> os)
          val jsonString = map2Json(map)
          res += jsonString
        } else {
          if (check) {
            dirtyRowkey += row
          }
        }
      } else {
        if (check) {
          dirtyRowkey += row
        }
      }
      if (res.length >= numberOfFile) {
        outToFile(res.toArray, s"$outPath$cnt")
        println(s"#####now get ${cnt * numberOfFile} records in hbase....")
        res.clear()
        cnt += 1
        Thread.sleep(1000)
      }
      if (check) {
        if (dirtyColumn.length >= numberOfFile) {
          outToFile(dirtyColumn.toArray, s"${outPath}_dirty_columns_$dirtyColumnCnt")
          println(s"#####now having found ${dirtyColumnCnt * numberOfFile} dirty columns in hbase....")
          dirtyColumn.clear()
          dirtyColumnCnt += 1
          Thread.sleep(1000)
        }
        if (dirtyRowkey.length >= numberOfFile) {
          outToFile(dirtyRowkey.toArray, s"${outPath}_dirty_rowkeys_$dirtyRowkeyCnt")
          println(s"#####now having found ${dirtyRowkeyCnt * numberOfFile} dirty rowkey in hbase....")
          dirtyRowkey.clear()
          dirtyRowkeyCnt += 1
          Thread.sleep(1000)
        }
      }
    }
    outToFile(res.toArray, s"$outPath$cnt")
    println(s"######there are ${(cnt - 1) * numberOfFile + res.length} records in hbase")
    if (check) {
      outToFile(dirtyColumn.toArray, s"${outPath}_dirty_columns_$dirtyColumnCnt")
      println(s"#####now having found ${(dirtyColumnCnt - 1) * numberOfFile + dirtyColumn.length} dirty columns in hbase....")
      outToFile(dirtyRowkey.toArray, s"${outPath}_dirty_rowkeys_$dirtyRowkeyCnt")
      println(s"#####now having found ${(dirtyRowkeyCnt - 1) * numberOfFile + dirtyRowkey.length} dirty rowkey in hbase....")
    }
  }

  def scanTable2JsonArray(table: HTable, outPath: String): Int = {
    val res = mutable.Buffer[String]()
    var cnt = 0
    val results: ResultScanner = table.getScanner(new Scan())
    val it = results.iterator()
    while (it.hasNext) {
      val map = mutable.Map[String, Any]()
      val next: Result = it.next()
      val row = Bytes.toString(next.getRow)
      if (row.contains(":")) {
        val rowkey = row.substring(0, row.indexOf(":"))
        if (rowkey.length == 32 || rowkey.length == 36) {
          map += ("did" -> (if (rowkey.contains("-")) rowkey.toUpperCase else rowkey))
          val os = if (rowkey.contains("-")) 1 else 0
          val value = mutable.ArrayBuffer[String]()
          for (kv <- next.raw()) {
            //map += (new String(kv.getFamily) + ":" + new String(kv.getQualifier) -> new String(kv.getValue))
            value += new String(kv.getQualifier)
          }
          cnt += 1
          map += ("tags" -> value.toArray)
          map += ("os" -> os)
          val jsonString = map2Json(map)
          res += jsonString
        }
        if (cnt % 100000 == 0 && cnt > 0) {
          outToFile(res.toArray, outPath + cnt / 100000)
          res.clear()
          Thread.sleep(1000)
        }
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
    var check = false
    var scan = false
    var delete = false
    var timestamp = false
    var path = ""
    var qulifier = false
    var prefix = ""

    val opt = new Options()
    opt.addOption("c", "check", false, "find illegal rowkey or column name from hbase ")
    opt.addOption("d", "delete", false, "delete records of file from hbase ")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("j", "json", false, "export data of hbase as json file")
    opt.addOption("n", "name", true, "the table name is used in hbase")
    opt.addOption("p", "path", true, "the file of records will be deleted from hbase")
    opt.addOption("r", "rowkey", true, "the path of file contains the rowkey will be got from hbase")
    opt.addOption("s", "scan", false, "scan the table with result being string")
    opt.addOption("ts", "timestamp", false, "scan the hbase table for getting last update timestamp")
    opt.addOption("t", "tag", true, "the path of file contains the tags used for filtering")
    opt.addOption("q", "qualifier", false, "scan the hbase table with qualifier prefix")
    opt.addOption("qp", "qualifier-prefix", true, "the qualifier prefix value")

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
    if (cl.hasOption("c")) check = true
    if (cl.hasOption("d")) delete = true
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("j")) json = true
    if (cl.hasOption("n")) table = cl.getOptionValue("n")
    if (cl.hasOption("p")) path = cl.getOptionValue("p")
    if (cl.hasOption("r")) rowkeyPath = cl.getOptionValue("r")
    if (cl.hasOption("s")) scan = true
    if (cl.hasOption("t")) filterTagPath = cl.getOptionValue("t")
    if (cl.hasOption("ts")) timestamp = true
    if (cl.hasOption("q")) qulifier = true
    if (cl.hasOption("qp")) prefix = cl.getOptionValue("qp")

    println(s"#####rowkeyPath = $rowkeyPath, filterTagPath = $filterTagPath, tableName = $table, " +
      s"outjson = $json, scan = $scan, with qualifier = $qulifier, prefix = $prefix, " +
      s"fortimestamp = $timestamp, check = $check, delete = $delete, delete path = $path")

    val conf = getConfiguration
    conf.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 180000)
    val hTable = getHTable(conf, table)

    if (json) {
      val count = scanTable2JsonArray(hTable, "jsonOfHbase")
      println(s"#####finish...there are $count records in table $tableName")
    } else if (timestamp) {
      println(s"#####start to scan hbase for timestamp")
      scanTable4Timestamp(hTable, "Hbase")
      println(s"#####scan finish...")
    } else if (scan) {
      if (qulifier) {
        println(s"#####start to scan hbase with qualifier prefix")
        //scanTable4Dirty(hTable, "illegal")
        scanTableWithColumnPrefix(hTable, prefix, "rowkeyWithColumn")
        println(s"#####scan with qualifier prefix finish...")
      } else {
        println(s"#####start to scan hbase")
        //scanTable4Dirty(hTable, "illegal")
        scanTable(hTable, "jsonOfHbase", check)
        println(s"#####scan finish...")
      }
    } else if (delete) {
      val deleteRowkeys = inFromFile(path)
      val groupRowkeys = deleteRowkeys.grouped(1000)
      groupRowkeys.foreach(r => {
        delMultiRows(hTable, r)
        println(s"#####there are 1000 records having been deleted from $tableName...")
      })
      println(s"#####deleting records from $tableName finish...")
    } else {
      val tags = inFromFile(filterTagPath)
      val tagsArray = tags(0).split(",").toList
      if (tagsArray.isEmpty) {
        println("#####Wrong: the filter tags are empty, exit....")
        System.exit(1)
      }
      process(hTable, rowkeyPath, tagsArray)
    }
    println("#####finished....")
  }
}
