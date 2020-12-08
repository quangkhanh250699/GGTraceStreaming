package com.hust.modeltrainer.iostream

import org.apache.spark.ml.PipelineModel

object SparkDumper extends Dumper {
  def dump(model: PipelineModel, path: String): Unit =
    try {
      model.save(path)
    } catch {
      case _ => model.write.overwrite.save(path)
    }

}
