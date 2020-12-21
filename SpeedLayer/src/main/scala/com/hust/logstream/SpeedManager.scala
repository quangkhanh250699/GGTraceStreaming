package com.hust.logstream

import com.hust.logstream.streaming.TaskEventStreaming

object SpeedManager {

  private val sparkSession = SparkEntry.sparkSession

  import sparkSession.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  val manager = TaskEventStreaming()

  def monitor: Unit = {
    manager.start
    manager.showStatistics
  }
}
