package fetcher.source
import java.util.Properties

import fetcher.info.{FetcherInfo, KafkaInfo}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object KafkaConnector extends SourceConnector{

  private def createConsumer(kafkaInfo: KafkaInfo): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInfo.KAFKA_BROKER)
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaInfo.MAX_POLL_INTERVAL_MS)
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaInfo.REQUEST_TIMEOUT_MS)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaInfo.SESSION_TIMEOUT_MS)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaInfo.AUTO_OFFSET_RESET)
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaInfo.FETCH_MIN_BYTES)
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafkaInfo.FETCH_MAX_WAIT_MS)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaInfo.ENABLE_AUTO_COMMIT)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaInfo.GROUP_ID_CONFIG)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    new KafkaConsumer[String, String](props)
  }

  override def getConsumer(fetcherInfo: FetcherInfo): KafkaConsumer[String, String] = {
    fetcherInfo match {
      case fetcherInfo: KafkaInfo => createConsumer(fetcherInfo)
      case _ => throw new Error("Not kafka config!!!")
    }
  }
}
