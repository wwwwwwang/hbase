package com.madhouse.dsp.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter.ofPattern
import java.time.temporal.ChronoUnit

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Functions {
  def dateParse(s:String, e:String):ArrayBuffer[String]={
    val df = ofPattern("yyyyMMdd")
    val res : ArrayBuffer[String] = new ArrayBuffer[String]()
    val start = LocalDate.parse(s, df)
    val end = LocalDate.parse(e, df)
    val dates = start.until(end, ChronoUnit.DAYS)
    for(i <- 0 to dates.toInt){
      val d=start.plusDays(i)
      val dateString = df.format(d)
      res += dateString
    }
    res
  }

  def allEqual(a:Int*):Boolean={
    var res = true
    for(i <- 0 until a.length-1){
      if(a(i) != a(i+1)){
        res = false
      }
    }
    res
  }

  val getOs: (String => String) = (id:String) =>{
    if(id.contains("-")) "1" else "0"
  }

  def inFromFile(path: String): Array[String] = {
    val source = Source.fromFile(path)
    val lines = source.getLines.toArray
    source.close
    println(s"#####read finishes. In file $path, there are ${lines.length} records....")
    lines
  }
}
