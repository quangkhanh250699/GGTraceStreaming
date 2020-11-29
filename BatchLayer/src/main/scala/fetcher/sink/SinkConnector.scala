package fetcher.sink

import fetcher.intermediary.Data

trait SinkConnector {
  def retrieve(data: Data): Unit = ???

  def close(): Unit = ???
}
