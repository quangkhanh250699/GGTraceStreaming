package com.hust.modeltrainer

import org.apache.spark.sql.SparkSession

object SparkEntry {

  def sparkSession = SparkSession.builder
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
}
