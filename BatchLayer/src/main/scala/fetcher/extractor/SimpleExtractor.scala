package fetcher.extractor
import fetcher.intermediary.Data

class SimpleExtractor extends Extractor{
  override def extract(data: Data): Data = {
    data
  }
}
