import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


private val model_path = "hdfs://localhost:9000/data/models/streaming.task-event"
private val kafka_broker = "localhost:9092"
private val topic = "TASK-EVENT"
private val sparkSession = SparkSession.builder.appName("a")
  .master("local")
  .getOrCreate()

import sparkSession.implicits._

sparkSession.sparkContext.setLogLevel("ERROR")
lazy private val model = PipelineModel.read.load(model_path)

val df = sparkSession.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_broker)
  .option("subscribe", topic)
  .option("includeHeaders", "false")
  .load()

val query = df.select(col("value"))
  .as[String].map((x: String) => x.split(","))

//  val query = df.select(col("value"))
//    .as[String].map((x: String) => x.split(","))
//    .writeStream
//    .outputMode("append")
//    .format("console")
//    .option("numRows", "5")
//    .start()

//  query.awaitTermination()

//  print(com.hust.logstream.model.toString())