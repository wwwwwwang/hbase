package com.madhouse.dsp

import com.madhouse.dsp.utils.Configer._
import com.madhouse.dsp.utils.Functions._
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

import scala.collection.JavaConverters._

/**
  * Created by Madhouse on 2017/8/28.
  */
object HbaseOption {

  def getConfiguration: Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    println(s"####zookeeperQuorum=$zookeeperQuorum, zookeeperPort=$zookeeperPort")
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
      println(s"table :$tableName is exist!")
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      admin.createTable(tableDesc)
      println(s"table :$tableName is created successfully")
    }
  }

  def deleteTable(admin: HBaseAdmin, tableName: String): Unit = {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("drop table successfully!")
    } else {
      println(s"table $tableName is not exist！")
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

  def main(args: Array[String]): Unit = {
    var table = tableName
    var column = ""
    var delete = false
    var path = ""

    val opt = new Options()
    opt.addOption("c", "column", true, "the column will be delete from hbase")
    opt.addOption("d", "delete", false, "delete records of file from hbase")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("n", "name", true, "the table name is used in hbase")
    opt.addOption("p", "path", true, "the path of file ")

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
    if (cl.hasOption("c")) column = cl.getOptionValue("c")
    if (cl.hasOption("d")) delete = true
    if (cl.hasOption("n")) table = cl.getOptionValue("n")
    if (cl.hasOption("p")) path = cl.getOptionValue("p")

    println(s"####tableName = $table, delete = $delete, path = $path, column = $column")

    val conf = getConfiguration
    val hTable = getHTable(conf, table)

    val deleteRowkeys = inFromFile(path)
    deleteRowkeys.foreach(r => {
      val delrow = r.toLowerCase + ":" + (if (r.contains("-")) "ifa" else "didmd5")
      delRowWithColumn(hTable, delrow, "cf", column)
    })
    println(s"#####deleting records from $table finish...")
  }
}
