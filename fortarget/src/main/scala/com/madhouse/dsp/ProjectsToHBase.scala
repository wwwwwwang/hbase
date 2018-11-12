package com.madhouse.dsp

import com.madhouse.dsp.utils.Configer._
import com.madhouse.dsp.utils.Saver._
import com.madhouse.dsp.utils.Functions._
import org.apache.commons.cli._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import com.madhouse.dsp.utils.Entity.ID
import org.apache.spark.sql.functions._

object ProjectsToHBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var tagStr = ""
    var savePath = ""
    var projects = ""
    var impUse = ""
    var clkUse = ""
    var cidOrCrid = ""
    var ids = ""
    var startDate = ""
    var endDate = ""
    var save2Hdfs = true
    var save2HBase = true

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("s", "string", true, "tag string")
    opt.addOption("p", "path", true, "the path that dataframe will be saved to")
    opt.addOption("ps", "projects", true, "all the projects in the one target dealing with, concated by ','")
    opt.addOption("i", "imp", true, "whether use imp logs to extract deviceid, 1:use, 0:no use, concated by ','")
    opt.addOption("c", "clk", true, "whether use clk logs to extract deviceid, 1:use, 0:no use, concated by ','")
    opt.addOption("cid", "cid-or-crid", true, "which one is used for filtering, 0:campaign id, 1:createive id")
    opt.addOption("l", "list", true, "the list used for filtering, concated by ';'")
    opt.addOption("sd", "start-date", true, "start dates of all project, concated by ','")
    opt.addOption("ed", "end-date", true, "end dates of all project, concated by ','")
    opt.addOption("u", "unsaved", false, "whether save the parquet data of device id to hdfs, using to import to beijing hbase")
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
    if (cl.hasOption("p")) savePath = cl.getOptionValue("p")
    if (cl.hasOption("ps")) projects = cl.getOptionValue("ps")
    if (cl.hasOption("i")) impUse = cl.getOptionValue("i")
    if (cl.hasOption("c")) clkUse = cl.getOptionValue("c")
    if (cl.hasOption("cid")) cidOrCrid = cl.getOptionValue("cid")
    if (cl.hasOption("l")) ids = cl.getOptionValue("l")
    if (cl.hasOption("sd")) startDate = cl.getOptionValue("sd")
    if (cl.hasOption("ed")) endDate = cl.getOptionValue("ed")
    if (cl.hasOption("u")) save2Hdfs = false
    if (cl.hasOption("uh")) save2HBase = false

    println(s"#####Tag String = $tagStr, \ndataframe Path = $savePath,\n" +
      s"projects= $projects, \nimpUse= $impUse, \nclkUse = $clkUse, \n" +
      s"cidOrCrid = $cidOrCrid, \nids = $ids, \nstartDate = $startDate,\n" +
      s"endDate = $endDate, \nsave2Hdfs=$save2Hdfs, \nsave2HBase = $save2HBase")

    val projestList = projects.split(",")
    val impList = impUse.split(",")
    val clkList = clkUse.split(",")
    val idTypeList = cidOrCrid.split(",")
    val idList = ids.split(";")
    val startDateList = startDate.split(",")
    val endDateList = endDate.split(",")

    require(allEqual(projestList.length, impList.length, clkList.length, idTypeList.length,
      idList.length, startDateList.length, endDateList.length),
      "all the parameters array length of project(imp,clk,id,starDate,endDate) should be same with projects array length")

    val impPath = "/madplatform/analytics/bid_imp2/day="
    val clkPath = "/madplatform/analytics/bid_clk2/day="

    for (p <- projestList) {
      println(s"project: $p start...")
      val index = projestList.indexOf(p)
      val dateList = dateParse(startDateList(index), endDateList(index))
      val typeString = if(idTypeList(index).toInt == 0) "cid" else "creative_id"
      val filterString = s"$typeString in (${idList(index)})"
      println(s"filter string = $filterString")
      for(d <- dateList){
        println(s"start to process: $d")
        if(impList(index).toInt ==1){
          val imp = sqlContext.read.avro(s"$impPath$d/*")
          imp.filter(s"$filterString").select("uid")
            .coalesce(10).write.mode("overwrite").format("text").save(s"$basePathOfLog/imp/$p/$d")
        }
        if(clkList(index).toInt ==1){
          val clk = sqlContext.read.avro(s"$clkPath$d/*")
          clk.filter(s"$filterString").select("uid")
            .coalesce(10).write.mode("overwrite").format("text").save(s"$basePathOfLog/clk/$p/$d")
        }
        println(s"process finish: $d")
      }
      println(s"project: $p finish...")
    }
    println(s"all projects imp/clk logs are processed to deviceid and saved into hdfs..")

    val GetOs = udf(getOs)

    val deviceId = sc.textFile(s"$basePathOfLog/{imp,clk}/{$projects}/*")
    val d = deviceId.map(r=>ID(r.toLowerCase.trim)).toDF.dropDuplicates()
    val dfs = d.withColumn("os",GetOs(d("id"))).cache()

    println(s"there are ${dfs.count} device ids..")
    dfs.show(20,truncate = false)
    dfs.groupBy('os).count().show()

    if(save2Hdfs){
      println(s"start to save dataframe to hdfs: $savePath")
      dfs.coalesce(20).write.format("parquet").mode("overwrite").save(s"$savePath")
      println(s"save to hdfs finish...")
    }

    if(save2HBase){
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
    println("finish...")
  }
}
