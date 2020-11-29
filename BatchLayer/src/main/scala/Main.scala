import fetcher.{Fetcher, KafkaFetcher}
import fetcher.info.{HDFSInfo, KafkaInfo}

object Main extends App{
  val sourceInfo1 = new KafkaInfo("TASK-EVENT")
  val sinkInfo1 = new HDFSInfo("/data/task-event", "task_event")
  val fetcher1 = new KafkaFetcher(sourceInfo1, sinkInfo1)

  val sourceInfo2 = new KafkaInfo("TASK-USAGE")
  val sinkInfo2 = new HDFSInfo("/data/task-usage", "task_event")
  val fetcher2 = new KafkaFetcher(sourceInfo2, sinkInfo2)

  val manager = new LogBatchManager(List[Fetcher](fetcher1, fetcher2))
  try {
    manager.run
  } catch {
    case _: Throwable => manager.stop
  }
}
