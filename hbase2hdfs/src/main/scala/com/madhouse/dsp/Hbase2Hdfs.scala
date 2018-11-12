package com.madhouse.dsp

import com.madhouse.dsp.Utils.Configer._
import com.madhouse.dsp.Utils._
import org.apache.commons.cli._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/8/24.
  */
object Hbase2Hdfs {
  val  log: Log = LogFactory.getLog(Hbase2Hdfs.getClass)

  def scanHbase(sc: SparkContext): RDD[Result] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //val count = hbaseRDD.count()
    //println("HbaseRDD Count:" + count)
    hbaseRDD.map(_._2)
  }

  def scanHbase(sc: SparkContext, tableName: String): RDD[Result] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //val count = hbaseRDD.count()
    //println("HbaseRDD Count:" + count)
    hbaseRDD.map(_._2)
  }

  def scanHbaseArray(sqlContext: HiveContext, rdd: RDD[Result]): DataFrame = {
    import sqlContext.implicits._
    val tagRdd = rdd.map(e => {
      val key = Bytes.toString(e.getRow)
      val rowkey = key.substring(0, key.lastIndexOf(":"))
      val a = e.raw()
      val tag: ArrayBuffer[String] = new ArrayBuffer[String]()
      for (cell <- a) {
        tag += new String(cell.getQualifier).substring(3)
      }
      TagWithArray(rowkey, tag)
    }).toDF
    tagRdd
  }

  def scanHbaseString(sqlContext: HiveContext, rdd: RDD[Result]): DataFrame = {
    import sqlContext.implicits._
    val tagRdd = rdd.map(e => {
      val key = Bytes.toString(e.getRow)
      val rowkey = key.substring(0, key.lastIndexOf(":"))
      val a = e.raw()
      var str = ""
      for (cell <- a) {
        str += new String(cell.getQualifier).substring(3) + ","
      }
      TagWithString(rowkey, str.substring(0, str.lastIndexOf(",")))
    }).toDF
    tagRdd
  }

  def scanHbaseFlat(sqlContext: HiveContext, rdd: RDD[Result]): DataFrame = {
    import sqlContext.implicits._
    val tagRdd = rdd.flatMap(e => {
      val key = Bytes.toString(e.getRow)
      val rowkey = key.substring(0, key.lastIndexOf(":"))
      val a = e.raw()
      //val tag: ArrayBuffer[OneTag] = new ArrayBuffer[OneTag]()
      a.map(cell => {
        OneTag(rowkey, new String(cell.getQualifier).substring(3))
      })
    }).toDF
    tagRdd
  }

  def scanHbaseFlat4Update(sqlContext: HiveContext, rdd: RDD[Result]): DataFrame = {
    import sqlContext.implicits._
    val updateRdd = rdd.flatMap(e => {
      val key = Bytes.toString(e.getRow)
      val rowkey = key
      val a = e.raw()
      a.map(cell => {
        OneUpdateNew(rowkey, new String(cell.getQualifier), cell.getTimestamp)
      })
    }).toDF
    updateRdd
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    var saveAsString = false
    var saveAsArray = false
    //var reverse = false
    var forUpdate = false

    val opt = new Options()
    opt.addOption("a", "array", false, "save the tags as array format")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("s", "string", false, "save the tags as string format")
    //opt.addOption("r", "reverse", false, "save the tags using tag as key")
    opt.addOption("u", "update", false, "deal with update")

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
    if (cl.hasOption("s")) saveAsString = true
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("a")) saveAsArray = true
    //if (cl.hasOption("r")) reverse = true
    if (cl.hasOption("u")) forUpdate = true

    log.info(s"#####saveAsString = $saveAsString, saveAsArray = $saveAsArray, forUpdate= $forUpdate")

    if (forUpdate) {
      log.info(s"#####start to deal with update timestamp....")
      //val scanRdd = scanHbase(sc, "madhouse_blueclick_data_code_timestamp")
      val scanRdd = scanHbase(sc, "maddsp_multiplefusion_data")
      val df = scanHbaseFlat4Update(sqlContext, scanRdd)
      //val res = df.filter("timeStamp < 1502640000000 and timeStamp > 9466560000000").withColumn("date",functions.unix_timestamp(df("timeStamp").substr(0,10)))
      val res = df.filter("tag like 'kd%'").withColumn("date", functions.from_unixtime(df("timeStamp").substr(0,10).cast("long"),"yyyy-MM-dd HH:mm:ss"))
        .sort(df("timeStamp").desc).cache()
      /*val res1 = df.filter(df("tag").contains("kd_")).withColumn("date", functions.from_unixtime(df("timeStamp").substr(0,10).cast("long"),"yyyy-MM-dd HH:mm:ss"))
        .sort(df("timeStamp").desc)*/
      //log.info(s"#####res.count = ${res.count()}")
      //res.show(100, truncate = false)
      res.write.mode("overwrite").format("parquet").save("/tmp/hbaseUpdate")
      log.info(s"#####deal with update timestamp finish....")
    } else {
      val scanRdd = scanHbase(sc).cache()
      log.info(s"scanRdd.count = " + scanRdd.count())
      if (saveAsString) {
        val df = scanHbaseString(sqlContext, scanRdd)
        //val df = scanHbaseString(sqlContext, scanRdd).sample(withReplacement = false,0.00001)
        df.printSchema
        df.show(20, truncate = false)
        log.info("#####start to save res df with string format to hdfs.....")
        df.coalesce(100).write.mode("overwrite").format("parquet").save(pathOfString)
        log.info("#####save res df to hdfs finished.....")
      }
      if (saveAsArray) {
        val df = scanHbaseArray(sqlContext, scanRdd)
        //val df = scanHbaseString(sqlContext, scanRdd).sample(withReplacement = false,0.00001)
        df.printSchema
        df.show(20, truncate = false)
        log.info("#####start to save res df with array format to hdfs.....")
        df.coalesce(100).write.mode("overwrite").format("parquet").save(pathOfArray)
        log.info("#####save res df to hdfs finished.....")
      }
      /*if (reverse) {
        val df = scanHbaseFlat(sqlContext, scanRdd)
        df.printSchema
        df.show(20, truncate = false)
        df.registerTempTable("test")
        //val sql = "select tag, concat_ws(',',collect_set(rowkey)) as rowkeys from test group by tag"
        //val sql = "select tag, collect_set(rowkey) as rowkeys from test group by tag"
        val sql = "select tag, count(distinct rowkey) as cnt from test group by tag order by cnt desc limit 200"
        val reverseDF = sqlContext.sql(sql)
        reverseDF.printSchema
        reverseDF.show(200, truncate = false)
        log.info("#####start to save reverse dataframe to hdfs.....")
        //reverseDF.coalesce(100).write.mode("append").format("parquet").save(pathOfArray)
        log.info("#####save reverse dataframe finished.....")
      }*/
      if (!saveAsArray && !saveAsString) {
        log.info("#####both save formats are false, exit directly.....")
      }
    }
  }
}
