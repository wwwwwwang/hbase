package com.madhouse.dsp

import com.madhouse.dsp.utils.Configer._
import com.madhouse.dsp.utils.Saver._
import org.apache.commons.cli._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataframeToHBase {

  def main(args: Array[String]): Unit = {
    var tagStr = ""
    var dfPath = ""
    var level = "WARN"
    var reTarget = false

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("s", "string", true, "tag string")
    opt.addOption("p", "path", true, "dataframe path")
    opt.addOption("l", "log-level", true, "log level:INFO, WARN")
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
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("s")) tagStr = cl.getOptionValue("s")
    if (cl.hasOption("p")) dfPath = cl.getOptionValue("p")
    if (cl.hasOption("l")) level = cl.getOptionValue("l")
    if (cl.hasOption("t")) reTarget = true

    println(s"#####Tag String = $tagStr, \ndataframe Path = $dfPath,\n" +
      s"reTarget = $reTarget, log-level = $level")

    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(level)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(dfPath).cache()

    println(s"there are ${df.count} device ids..")
    df.show(20, truncate = false)
    df.groupBy('os).count().show()

    println("start to save device ids to hbase...")

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
    println("save to hbase finish...")
  }
}
