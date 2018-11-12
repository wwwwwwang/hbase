package com.madhouse.dsp.utils

import com.madhouse.dsp.utils.Configer.{tableName, zookeeperPort, zookeeperQuorum}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Saver {
  def saveHBase(df: DataFrame)(func: (DataFrame) => RDD[(ImmutableBytesWritable, Put)]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.master", "10.10.16.13:16000")
    println(s"#####tableName = $tableName, zookeeperQuorum = $zookeeperQuorum")
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    func(df).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def saveHBase(df: DataFrame, str: String)(func: (DataFrame, String) => RDD[(ImmutableBytesWritable, Put)]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    println(s"#####tableName = $tableName, zookeeperQuorum = $zookeeperQuorum")
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    func(df, str).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
