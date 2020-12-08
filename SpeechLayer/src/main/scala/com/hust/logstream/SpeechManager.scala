package com.hust.logstream

import com.hust.logstream.streaming.TaskEventStreaming

object SpeechManager {

  private val model_path = "hdfs://hadoop-namenode:8020/data/models/task-event"
  private val kafka_broker = "kafka:9093"
  private val topic = "TASK-EVENT"
  private val sparkSession = SparkEntry.sparkSession

  import sparkSession.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  val manager = TaskEventStreaming()

  def monitor: Unit = {

    manager.start
    manager.showStatistics

    while(true) {
      sparkSession.sql("select cpuRequest, memoryRequest, diskSpaceRequest from task_event").show()
      Thread.sleep(3000)
    }

  }
}
