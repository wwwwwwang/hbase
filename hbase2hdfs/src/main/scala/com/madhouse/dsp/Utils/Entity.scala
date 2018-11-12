package com.madhouse.dsp.Utils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/8/24.
  */
case class TagWithString(rowkey: String, tags: String)

case class TagWithArray(rowkey: String, tags: ArrayBuffer[String])

case class OneTag(rowkey: String, tag: String)

case class OneUpdate(rowkey: String, timeStamp: Long)

case class OneUpdateNew(rowkey: String, tag: String, timeStamp: Long)


