package com.madhouse.dsp

import com.madhouse.dsp.Utils.Configer.{sparkAppName, tableName, zookeeperPort, zookeeperQuorum}
import org.apache.commons.cli._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by Madhouse on 2017/9/27.
  */
object Df2Hbase {
  val log: Log = LogFactory.getLog(Df2Hbase.getClass)

  private def saveHBase(df: DataFrame)(func: (DataFrame) => RDD[(ImmutableBytesWritable, Put)]) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    //conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    log.info(s"#####tableName = $tableName")
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    func(df).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  private def saveHBase(df: DataFrame, str: String)(func: (DataFrame, String) => RDD[(ImmutableBytesWritable, Put)]) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    log.info(s"#####tableName = $tableName")
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    func(df, str).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    //var tagStr = "023014"
    var tagStr = "023019"
    var dfPath = "/tmp/whsh/retargeting"
    var reTarget = false

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("s", "string", true, "tag string")
    opt.addOption("p", "path", true, "dataframe path")
    //opt.addOption("r", "reverse", false, "save the tags using tag as key")
    opt.addOption("t", "target", false, "deal with the dataframe for retarget")

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
    if (cl.hasOption("s")) tagStr = cl.getOptionValue("s")
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("p")) dfPath = cl.getOptionValue("p")
    //if (cl.hasOption("r")) reverse = true
    //if (cl.hasOption("u")) forUpdate = true
    if (cl.hasOption("t")) reTarget = true

    log.info(s"#####Tag String = $tagStr, dataframe Path = $dfPath, deal with for retarget: $reTarget")


    //val df = sqlContext.read.parquet(dfPath).sample(withReplacement = false, 0.0001).cache()
    val df = sqlContext.read.parquet(dfPath).cache()

    df.show(20, truncate = false)

    if (reTarget) {
      //for retarget
      saveHBase(df, tagStr) {
        (df, tagStr) =>
          df.map { r =>
            val uid = r.getString(0).toLowerCase + ":" + (if (r.getString(1) == "0") "didmd5" else "ifa")
            val tag = tagStr
            val put = new Put(Bytes.toBytes(uid))
            put.add(Bytes.toBytes("cf"), Bytes.toBytes(s"ap_$tag"), Bytes.toBytes("1"))
            (new ImmutableBytesWritable, put)
          }
      }
    } else {
      //for adding dataframe(string,string(splitby,)) to hbase
      saveHBase(df) {
        (df) =>
          df.map { r =>
            val uid = r.getString(0).toLowerCase
            //val rowkey = if (uid.contains("-")) uid + ":ifa" else uid + ":didmd5"
            val values = r.getString(1).split(",")
            val put = new Put(Bytes.toBytes(uid))
            for (v <- values) {
              put.add(Bytes.toBytes("cf"), Bytes.toBytes(s"$v"), Bytes.toBytes("1"))
            }
            (new ImmutableBytesWritable, put)
          }
      }
    }

  }
}
