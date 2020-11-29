package fetcher.info

class KafkaInfo(val TOPIC: String) extends FetcherInfo {
  val KAFKA_BROKER = "localhost:9092"
  val AUTO_OFFSET_RESET = "latest"
  val OFFSET_RESET_EARLIER = "earliest"
  val GROUP_ID_CONFIG = "TASK-GROUP"
  val MESSAGE_COUNT = "1000"
  val MAX_POLL_INTERVAL_MS = "10000"
  val MAX_NO_MESSAGE_FOUND_COUNT = "100"
  val FETCH_MIN_BYTES = "4096"
  val SESSION_TIMEOUT_MS = "180000"
  val REQUEST_TIMEOUT_MS = "30000"
  val FETCH_MAX_WAIT_MS = "10000"
  val ENABLE_AUTO_COMMIT = "false"
}
