package com.hust.modeltrainer.iostream

import org.apache.spark.sql.DataFrame

import com.hust.modeltrainer.SparkEntry

object SparkLoader extends Loader {

  override def csv(path: String): DataFrame =
    SparkEntry.sparkSession.read.csv(path)

}
