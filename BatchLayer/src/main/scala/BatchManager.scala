import fetcher.info.FetcherInfo

trait BatchManager {
  def run: Unit = ???
  def addConnection(sourceInfo: FetcherInfo, sinkInfo: FetcherInfo): BatchManager = ???
  def stop: Unit = ???
}
