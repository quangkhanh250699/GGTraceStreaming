package fetcher

import fetcher.info.FetcherInfo

abstract class Fetcher(val sourceInfo: FetcherInfo, val sinkInfo: FetcherInfo) {
  def start: Unit = ???
  def stop: Unit = ???
}