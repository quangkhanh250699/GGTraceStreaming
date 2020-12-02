package com.hust.logstream

import com.hust.logstream.streaming.TaskEventStreaming

object SpeechManager {

  private val model_path = "hdfs://localhost:9000/data/models/streaming.task-event"
  private val kafka_broker = "localhost:9092"
  private val topic = "TASK-EVENT"
  private val sparkSession = SparkEntry.sparkSession

  import sparkSession.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  val manager = TaskEventStreaming

  def monitor: Unit = {

    manager.start

    //  manager.showStatistics

    while(true) {
      sparkSession.sql("select cpuRequest, memoryRequest, diskSpaceRequest from task_event").show()
      Thread.sleep(3000)
    }

  }
}
