package com.hust.logstream.streaming.machine

import com.hust.logstream.SparkEntry
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, col}

class MachineManager {

  private val sparkSession = SparkEntry.sparkSession

  def loadMachine(path: String): DataFrame = {
    val machines = sparkSession.read.option("header", "true").csv(path)
    return machines.groupBy("machine_id").agg(
      max("cpus").alias("cpu"),
      max("memory").alias("memory")
    )
  }

}
