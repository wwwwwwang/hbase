package com.madhouse.dsp

import com.madhouse.dsp.utils.Configer._
import com.madhouse.dsp.utils.Entity.ID
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.Saver._
import org.apache.commons.cli._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object FilesToHBase {
  def main(args: Array[String]): Unit = {
    var tagStr = ""
    var level = "WARN"
    var filePath = ""
    var save2Hdfs = true
    var save2HBase = true
    var identifier = ","
    var index = 0
    var savePath = ""

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("s", "string", true, "tag string")
    opt.addOption("p", "path", true, "hdfs path of the deviceid file")
    opt.addOption("i", "identifier", true, "the deviceid file will be splited by identifier")
    opt.addOption("l", "log-level", true, "log level:INFO, WARN")
    opt.addOption("n", "number", true, "the index of element will be toke from array produced by split")
    opt.addOption("u", "unsaved", false, "whether save the parquet data of device id to hdfs, using to import to beijing hbase")
    opt.addOption("sp", "save-path", true, "if save, the path to save deviceid dataframe")
    opt.addOption("uh", "unsaved-hbase", false, "whether save the parquet data of device id to hbase")

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
    if (cl.hasOption("s")) tagStr = cl.getOptionValue("s")
    if (cl.hasOption("p")) filePath = cl.getOptionValue("p")
    if (cl.hasOption("u")) save2Hdfs = false
    if (cl.hasOption("uh")) save2HBase = false
    if (cl.hasOption("l")) level = cl.getOptionValue("l")
    if (cl.hasOption("i")) identifier = cl.getOptionValue("i")
    if (cl.hasOption("n")) index = cl.getOptionValue("n").toInt
    if (cl.hasOption("sp")) savePath = cl.getOptionValue("sp")

    println(s"#####Tag String = $tagStr, \nfile Path = $filePath,\n" +
      s"identifier = $identifier, \nindex = $index, \n" +
      s"save2Hdfs = $save2Hdfs, \nsavePath = $savePath, \n" +
      s"save2HBase = $save2HBase, \nlog-level = $level")

    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(level)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val GetOs = udf(getOs)

    val deviceId = sqlContext.read.text(s"$filePath").map(r => r.mkString).filter(r => r.length >= 20)
    val d = deviceId.map(r => r.split(identifier)(index)).map(r => ID(r.toLowerCase.trim)).toDF.dropDuplicates()
    val dfs = d.withColumn("os", GetOs(d("id"))).cache()

    println(s"there are ${dfs.count} device ids..")
    dfs.show(20, truncate = false)
    dfs.groupBy('os).count().show()

    if (save2Hdfs) {
      println(s"start to save dataframe to hdfs: $savePath")
      dfs.coalesce(20).write.format("parquet").mode("overwrite").save(s"$savePath")
      println(s"save to hdfs finish...")
    }

    if (save2HBase) {
      println("start to save device ids to hbase...")
      saveHBase(dfs, tagStr) {
        (df, tagStr) =>
          df.map { r =>
            val uid = r.getString(0).toLowerCase + ":" + (if (r.getString(1) == "0") "didmd5" else "ifa")
            val tag = tagStr
            val put = new Put(Bytes.toBytes(uid))
            put.add(Bytes.toBytes("cf"), Bytes.toBytes(s"ap_$tag"), Bytes.toBytes("1"))
            (new ImmutableBytesWritable, put)
          }
      }
      println("save to hbase finish...")
    }
    println("finish..")
  }
}
