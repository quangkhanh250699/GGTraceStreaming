package com.hust.logstream.streaming

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, exp, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.hust.logstream.model.prediction.TaskEventModel
import com.hust.logstream.streaming.machine.MachineManager
import com.hust.logstream.streaming.stream.KafkaStream
import com.hust.logstream.streaming.task.TaskEvent

case class TaskEventStreaming() extends StreamManager {

  private val kafkaBroker = "kafka:9093"
  private val topic = "TASK-EVENT"
  private val sparkSession = SparkSession.builder.getOrCreate()

  override protected val stream: KafkaStream = new KafkaStream(kafkaBroker, topic)

  private val initDF: DataFrame = stream.streamReader.load()

  case class CountNumber(group: Int, number: Long)

  override def start: Unit = {
  }

  private def machineGroup = {
    import sparkSession.implicits._
    val machineManager = new MachineManager()
    val machinePath = "/opt/workspace/data/machine.csv"
    val machineDF = machineManager.loadMachine(machinePath)
    machineDF.show(3)
    val splitDF = machineDF.randomSplit(Array(0.4,0.3,0.3))
    val (df0,df1,df2) = (splitDF(0).withColumn("id",monotonically_increasing_id()),
      splitDF(1).withColumn("id",monotonically_increasing_id()),
      splitDF(2).withColumn("id",monotonically_increasing_id()))
    //    println(df1.count(), df2.count(), df3.count(), machineDF.count())

    val countDF = sparkSession.createDataFrame(Seq(
      (0, df0.count()),
      (1, df1.count()),
      (2, df2.count())
    )).select($"_1".cast("int").alias("group"), $"_2".cast("int").alias("number"))
    (df0, df1, df2, countDF)
  }

  private def extractDF: DataFrame = {
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

    extractedDF
  }

  override def getStatistics: Unit = super.getStatistics

  override def showStatistics: Unit = {
    import sparkSession.implicits._
    val extractedDF = extractDF
    val (df0, df1, df2, countDF) = machineGroup
    val df = extractedDF.select($"id".cast("int"),
                                $"cpuRequest".alias("_c10"),
                                $"memoryRequest".alias("_c11"))
    val prediction = TaskEventModel.predict(df)
    val predictionWithGroup = prediction.join(countDF, prediction.col("prediction") === countDF.col("group"), "inner")
      .withColumn("machineIndex", expr("id % number"))
    predictionWithGroup.writeStream
      .format("console")
      .outputMode("append")
      .start.awaitTermination()
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
