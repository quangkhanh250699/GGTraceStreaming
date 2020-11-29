package fetcher.info

class HDFSInfo(val DATA_PATH: String, val FILE_NAME: String) extends FetcherInfo {
  val DEFAULT_FS = "hdfs://localhost:9000"
}
