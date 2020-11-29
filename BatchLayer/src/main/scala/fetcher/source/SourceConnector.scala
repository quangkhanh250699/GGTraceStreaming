package fetcher.source

import fetcher.info.FetcherInfo

trait SourceConnector{
  def getConsumer(fetcherInfo: FetcherInfo): Any = ???
}
