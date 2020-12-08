package com.hust.logstream

import org.apache.spark.sql.SparkSession

object SparkEntry {
  //  def apply() = ???

  val sparkSession = SparkSession.builder.getOrCreate()
}
