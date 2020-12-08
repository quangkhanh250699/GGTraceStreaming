import com.hust.modeltrainer.SparkEntry
import com.hust.modeltrainer.trainers.TaskEventTrainer
import org.apache.spark.sql.SparkSession

object Main extends App {

  override def main(args: Array[String]): Unit = {
    args.foreach(println(_))
    val (master, modelPath, memory): (String, String, String) = args.length match {
      case 3 => {
        val master = args.head
        val modelPath = args(1)
        val memory = args(2)
        (master, modelPath, memory)
      }
      case _ => ("spark://spark-master:7077", "hdfs://hadoop-namenode:8020/data/model/task-event", "1024m")
    }

    val appName = "Trainer"
    val sparkSession = SparkSession.builder
      .master(master)
      .appName(appName)
      .config("spark.executor.memory", memory)
      .config("spark.driver.memory", memory)
      .getOrCreate()
    val trainer = new TaskEventTrainer(modelPath)
    trainer.train
  }
}
