package fetcher.extractor

import fetcher.intermediary.Data

abstract class Extractor {
  def extract(data: Data): Data = ???
}
