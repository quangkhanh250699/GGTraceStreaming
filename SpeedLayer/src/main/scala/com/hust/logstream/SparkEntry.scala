package com.hust.logstream

import org.apache.spark.sql.SparkSession

object SparkEntry {
  //  def apply() = ???
  private val master = "spark://spark-master:7077"
  private val appName = "SpeechLayer"

  val sparkSession = SparkSession.builder
    .master(master)
    .appName(appName)
    .getOrCreate()
}
