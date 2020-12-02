package com.hust.logstream

import org.apache.spark.sql.SparkSession

object SparkEntry {
  //  def apply() = ???

  private val appName = "Log Streaming"
  private val master = "local"

  val sparkSession = SparkSession.builder.appName(appName).master(master).getOrCreate()
}
