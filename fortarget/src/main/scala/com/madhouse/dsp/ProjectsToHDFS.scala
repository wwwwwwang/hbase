package com.madhouse.dsp

import com.databricks.spark.avro._
import com.madhouse.dsp.utils.Configer._
import com.madhouse.dsp.utils.Functions._
import org.apache.commons.cli._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ProjectsToHDFS {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(sparkAppName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)

    var startDate = ""
    var endDate = ""
    var hdfsPath = ""
    var impUse = ""
    var clkUse = ""
    var cidOrCrid = ""
    var ids = ""
    var selectFields = ""
    var format = "parquet"
    var identifier = ","

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("sd", "start-date", true, "start dates of all campaigns")
    opt.addOption("ed", "end-date", true, "end dates of all campaigns")
    opt.addOption("p", "path", true, "the path that dataframe will be saved to")
    opt.addOption("f", "fields", true, "the fields will be selected from dataframe")
    opt.addOption("i", "imp", true, "whether use imp logs to extract deviceid, 1:use, 0:no use, concated by ','")
    opt.addOption("c", "clk", true, "whether use clk logs to extract deviceid, 1:use, 0:no use, concated by ','")
    opt.addOption("cid", "cid-or-crid", true, "which one is used for filtering, 0:campaign id, 1:createive id")
    opt.addOption("l", "list", true, "the list used for filtering, concated by ';'")
    opt.addOption("t", "text", false, "save all record as text format")
    opt.addOption("s", "split-by", true, "when saved as text using as identifier")

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
    if (cl.hasOption("sd")) startDate = cl.getOptionValue("sd")
    if (cl.hasOption("ed")) endDate = cl.getOptionValue("ed")
    if (cl.hasOption("p")) hdfsPath = cl.getOptionValue("p")
    if (cl.hasOption("i")) impUse = cl.getOptionValue("i")
    if (cl.hasOption("c")) clkUse = cl.getOptionValue("c")
    if (cl.hasOption("cid")) cidOrCrid = cl.getOptionValue("cid")
    if (cl.hasOption("l")) ids = cl.getOptionValue("l")
    if (cl.hasOption("f")) selectFields = cl.getOptionValue("f")
    if (cl.hasOption("t")) format = "text"
    if (cl.hasOption("s")) identifier = cl.getOptionValue("s")

    println(s"#####startDate = $startDate, \nendDate = $endDate, \n" +
      s"dataframe Path = $hdfsPath,\nimpUse= $impUse, \nclkUse = $clkUse, \n" +
      s"cidOrCrid = $cidOrCrid, \nids = $ids, \nselectFields=$selectFields, \n" +
      s"format = $format, \nidentifier = $identifier")

    val hdfsPathList = hdfsPath.split(",")
    val impList = impUse.split(",")
    val clkList = clkUse.split(",")
    val idTypeList = cidOrCrid.split(",")
    val idList = ids.split(";")

    require(allEqual(hdfsPathList.length, impList.length, clkList.length,
      idTypeList.length, idList.length),
      "all the parameters array length of project(imp,clk,id,starDate,endDate) should be same with projects array length")

    val impPath = "/madplatform/analytics/bid_imp2/day="
    val clkPath = "/madplatform/analytics/bid_clk2/day="

    val dateList = dateParse(startDate, endDate)

    for (d <- dateList) {
      println(s"start to process: $d")
      for (i <- idList) {
        println(s"campaign/material: $i start...")
        val index = idList.indexOf(i)
        val typeString = if (idTypeList(index).toInt == 0) "cid" else "creative_id"
        val filterString = s"$typeString in (${idList(index)})"
        println(s"filter string = $filterString")
        if (impList(index).toInt == 1) {
          val imp = sqlContext.read.avro(s"$impPath$d/*").filter(s"$filterString")
          val r = if(!"".equalsIgnoreCase(selectFields)) imp.selectExpr(selectFields.split(","): _*) else imp
          if(format.equalsIgnoreCase("text")){
            println(s"####schema = ${r.schema.fieldNames.mkString(identifier)}")
            r.coalesce(10).map(r=>r.mkString(identifier)).saveAsTextFile(s"${hdfsPathList(index)}/imp/$d")
          }else{
            r.coalesce(10).write.mode("overwrite").format("parquet").save(s"${hdfsPathList(index)}/imp/$d")
          }
        }
        if (clkList(index).toInt == 1) {
          val clk = sqlContext.read.avro(s"$clkPath$d/*").filter(s"$filterString")
          val r = if(!"".equalsIgnoreCase(selectFields)) clk.selectExpr(selectFields.split(","): _*) else clk
          if(format.equalsIgnoreCase("text")){
            println(s"####schema = ${r.schema.fieldNames.mkString(identifier)}")
            r.coalesce(10).map(r=>r.mkString(identifier)).saveAsTextFile(s"${hdfsPathList(index)}/clk/$d")
          }else{
            r.coalesce(10).write.mode("overwrite").format("parquet").save(s"${hdfsPathList(index)}/clk/$d")
          }
        }
        println(s"campaign/material: $i finish...")
      }
      println(s"process finish: $d")
    }
    println(s"all campaigns/materials imp/clk logs are processed and saved into hdfs..")
  }
}
