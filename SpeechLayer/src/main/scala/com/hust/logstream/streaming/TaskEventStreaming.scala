package com.hust.logstream.streaming

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.col

import com.hust.logstream.model.prediction.TaskEventModel
import com.hust.logstream.streaming.stream.KafkaStream
import com.hust.logstream.streaming.task.TaskEvent

case class TaskEventStreaming() extends StreamManager {

  private val kafkaBroker = "kafka:9093"
  private val topic = "TASK-EVENT"
  private val sparkSession = SparkSession.builder.getOrCreate()

  override protected val stream: KafkaStream = new KafkaStream(kafkaBroker, topic)

  private val initDF: DataFrame = stream.streamReader.load()

  override def start: Unit = {
    import sparkSession.implicits._
    val extractedDF = initDF.select("value")
      .as[String].map((s: String) => s.split(","))
      .select(
        $"value".getItem(0).as("id"),
        $"value".getItem(1).as("time"),
        $"value".getItem(2).as("missingInfo"),
        $"value".getItem(3).as("jobId"),
        $"value".getItem(4).as("taskIndex"),
        $"value".getItem(5).as("machineId"),
        $"value".getItem(6).as("eventType"),
        $"value".getItem(7).as("user"),
        $"value".getItem(8).as("schedulingClass"),
        $"value".getItem(9).as("priority"),
        $"value".getItem(10).as("cpuRequest"),
        $"value".getItem(11).as("memoryRequest"),
        $"value".getItem(12).as("diskSpaceRequest"),
        $"value".getItem(13).as("differentMachineRestriction"),
      )

    extractedDF.writeStream
      .format("memory")
      .queryName("task_event")
      .outputMode("append")
      .option("numRows", "5")
      .start()
  }

  override def getStatistics: Unit = super.getStatistics

  override def showStatistics: Unit = {
    while (true) {
      val df = sparkSession.sql("select cpuRequest as _c10," +
        " memoryRequest as _c11, diskSpaceRequest as _c12 from task_event " +
        "ORDER BY id DESC LIMIT 5")
      val predicted_df = TaskEventModel.predict(df)
      predicted_df.show()
      Thread.sleep(5000)
    }
  }

  private def toTaskEvent(str: String): TaskEvent = {
    val fields = str.split(",")
    if (fields.length < 14) fakeTaskEvent
    else TaskEvent(
      toInt(fields(0)),
      toInt(fields(1)),
      toInt(fields(2)),
      toInt(fields(3)),
      toInt(fields(4)),
      toInt(fields(5)),
      toInt(fields(6)),
      fields(7),
      toInt(fields(8)),
      toInt(fields(9)),
      toFloat(fields(10)),
      toFloat(fields(11)),
      toFloat(fields(12)),
      toInt(fields(13))
    )
  }

  private def toFloat(s: String): Float =
    try {
      s.toFloat
    } catch {
      case _ => -1
    }

  private def toInt(s: String): Int =
    try {
      s.toInt
    } catch {
      case _ => -1
    }


  private def fakeTaskEvent: TaskEvent =
    TaskEvent(-1, -1, -1, -1, -1, -1, -1, "", -1, -1, -1, -1, -1, -1)
}
