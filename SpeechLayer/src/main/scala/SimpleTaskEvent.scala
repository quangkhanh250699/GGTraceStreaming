import com.hust.logstream.SparkEntry
import org.apache.spark.streaming.kafka010._
import org.apache.spark.ml.PipelineModel

class SimpleTaskEvent {

  private val model_path = "hdfs://localhost:9000/data/models/streaming.task-event"
  private val kafka_broker = "localhost:9092"
  private val topic = "TASK-EVENT"
  private val sparkSession = SparkEntry.sparkSession

  lazy private val model = PipelineModel.read.load(model_path)

  val df = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", topic)
    .option("includeHeaders", "false")
    .load()
}
