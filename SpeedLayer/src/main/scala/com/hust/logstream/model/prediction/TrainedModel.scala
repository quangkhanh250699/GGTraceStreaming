package com.hust.logstream.model.prediction

import org.apache.spark.sql.DataFrame

abstract class TrainedModel {

  def predict(df: DataFrame): DataFrame = ???

}
