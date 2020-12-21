package com.hust.logstream.streaming.stream

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.SparkSession

class KafkaStream(var kafkaBroker: String, var topic: String) extends Stream {

  private val sparkSession: SparkSession = SparkSession.builder.getOrCreate()

  override def initConnection: Unit = Unit

  val streamReader: DataStreamReader = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBroker)
    .option("subscribe", topic)
    .option("includeHeaders", "false")

  override def showBasicInfo: Unit = {
    print("kafka servers: " + kafkaBroker)
    print("topic : " + topic)
  }
}
