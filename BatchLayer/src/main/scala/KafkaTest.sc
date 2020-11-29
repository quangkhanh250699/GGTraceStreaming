import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.{Arrays, Properties}
import java.time.Duration
import scala.util.Try
import com.typesafe.scalalogging.Logger
import java.io.{PrintWriter, File}

val props = new Properties()
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")
//props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000")
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
val consumer = new KafkaConsumer[String, String](props)
consumer.subscribe(Arrays.asList("TASK-EVENT"))
val logger = Logger(getClass.getSimpleName)
val writer = new PrintWriter(new File("/media/quangkhanh/E/Coursera/SparkStreaming/BatchLayer/data.txt"))
print("start testing")
writer.write("now ---555-------")
val records = consumer.poll(Duration.ofMillis(10000))
records.forEach(r => writer.println(r.value()))
writer.close()
print("..............")
