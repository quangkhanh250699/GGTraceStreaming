package fetcher

import java.time.Duration
import java.util.Arrays

import fetcher.extractor.SimpleExtractor
import fetcher.info.{HDFSInfo, KafkaInfo}
import fetcher.sink.HDFSConnector
import fetcher.source.KafkaConnector
import fetcher.intermediary.Data
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

class KafkaFetcher(sourceInfo: KafkaInfo, sinkInfo: HDFSInfo)
  extends Fetcher(sourceInfo, sinkInfo) {

  val consumer: KafkaConsumer[String, String] = KafkaConnector.getConsumer(sourceInfo)
  val retriever = new HDFSConnector(sinkInfo)
  val extractor = new SimpleExtractor()

  override def start: Unit = {
    try {
      consumer.subscribe(Arrays.asList(sourceInfo.TOPIC))

      while (true) {
        val records = consumer.poll(Duration.ofMillis(10000))
        println("Number of " + sourceInfo.TOPIC + " records fetched: "
          + records.count() + " from partition " + records.partitions())
        records.forEach((r: ConsumerRecord[String, String]) => {
          val data = new Data(r.value())
          val extractedData = this.extractor.extract(data)
          this.retriever.retrieve(extractedData)
        })
      }
    } catch {
      case e: Exception => retriever.close()
    }
  }

  override def stop: Unit = {
    this.retriever.close()
  }
}
