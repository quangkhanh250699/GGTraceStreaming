package fetcher.sink
import java.io.{FileNotFoundException, PrintWriter}

import fetcher.info.{FetcherInfo, HDFSInfo}
import fetcher.intermediary.Data
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HDFSConnector(_info: FetcherInfo) extends SinkConnector{

  val info = _info.asInstanceOf[HDFSInfo]
  val maxCount = 10000

  private val defaultFS = info.DEFAULT_FS
  private val dataPath = info.DATA_PATH
  private val fileName = info.FILE_NAME

  private val extension = ".csv"

  private var fileCount = 0
  private var writeCount = 0


  private val conf: Configuration = {
    val _conf = new Configuration()
    _conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    _conf.set("fs.defaultFS", this.defaultFS)
    _conf
  }

  private var writer = getNewWriter

  private def getNewWriter = {
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(this.dataPath + "/" + this.fileName + fileCount.toString + this.extension))
    new PrintWriter(output)
  }

  private def getWriter: PrintWriter = {
    if (writeCount > maxCount) {
      writeCount = 0
      fileCount += 1
      this.writer.close()
      this.writer = getNewWriter
    }
    this.writer
  }

  override def retrieve(data: Data): Unit = {
    val writer = this.getWriter
    try {
      writer.write(data.value)
      writer.write("\n")
      writeCount += 1
    } catch {
      case e: FileNotFoundException => throw e
      case e: Exception => writer.close()
    }
  }

  override def close(): Unit = {
    this.writer.close()
  }
}
