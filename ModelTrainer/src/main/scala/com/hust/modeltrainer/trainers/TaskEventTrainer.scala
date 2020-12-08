package com.hust.modeltrainer.trainers

import com.hust.modeltrainer.iostream.{SparkDumper, SparkLoader}
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.{Estimator, Pipeline, Transformer}
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class TaskEventTrainer(var modelPath: String) extends Trainer {

  override def getDataPath: String =
    "hdfs://hadoop-namenode:8020/data/task-event/"

  override def getModelPath: String =
    modelPath

  override def train: Unit = {
    val inputDF = SparkLoader.csv(getDataPath).select("_c10", "_c11", "_c12")
    val inputDFCasted = inputDF.select(
      inputDF.columns.map((c: String) => col(c).cast("float")): _*
    ).limit(10000)
    val transformer = createTransformer(inputDFCasted)
    val predictor = createPredictedModel

    val pipeline = new Pipeline().setStages(Array(transformer, predictor))

    val model = pipeline.fit(inputDFCasted)

    model.transform(inputDFCasted).show(10)

    SparkDumper.dump(model, getModelPath)
  }

  def createPredictedModel: Estimator[GaussianMixtureModel] = {
    new GaussianMixture().setK(3)
  }

  def createTransformer(df: DataFrame): Pipeline = {
    val imputedComlumns = getImputedColumns(df)

    val imputer = new Imputer()
      .setInputCols(df.columns)
      .setOutputCols(imputedComlumns)
      .setStrategy("mean")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(imputedComlumns)
      .setOutputCol("features")

    new Pipeline().setStages(Array(imputer, vectorAssembler))
  }

  private def getImputedColumns(df: DataFrame): Array[String] =
    df.columns.map((c: String) => c + "Imputed")
}
