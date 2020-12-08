package com.hust.logstream.model.prediction

import com.hust.logstream.SparkEntry
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.PipelineModel

object TaskEventModel extends TrainedModel {

  private val sparkSession = SparkEntry.sparkSession

  private val model_path = "hdfs://hadoop-namenode:8020/data/model/task-event/"

  lazy private val model = PipelineModel.read.load(model_path)

  override def predict(df: DataFrame): DataFrame = {
    val new_df = df.select(
      df.columns.map(c => col(c).cast("float")): _*
    )
    model.transform(new_df)
  }
}
