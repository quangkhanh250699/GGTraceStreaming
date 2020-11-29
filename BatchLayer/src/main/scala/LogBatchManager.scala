import fetcher.{Fetcher, KafkaFetcher}
import fetcher.info.{FetcherInfo, HDFSInfo, KafkaInfo}

class LogBatchManager(_fetchers: List[Fetcher]) extends BatchManager {

  private val fetchers: List[Fetcher] = _fetchers

  override def run: Unit = {
    for (fet <- this.fetchers) {
      val thread = new Thread{
        override def run(): Unit = {
          fet.start
        }
      }
      thread.start()
    }
  }

  override def addConnection(sourceInfo: FetcherInfo, sinkInfo: FetcherInfo): BatchManager = {
    val newFetcher: Fetcher = (sourceInfo, sinkInfo) match {
      case (sourceInfo: KafkaInfo, sinkInfo: HDFSInfo) => new KafkaFetcher(sourceInfo, sinkInfo)
      case _ => throw new Exception("Cannot find this kind of fetcher")
    }

    new LogBatchManager(newFetcher :: this.fetchers)
  }

  override def stop: Unit = this.fetchers.foreach((fet: Fetcher) => fet.stop)
}
