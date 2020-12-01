# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __task_event_preprocessor.py
# project name: ModelManager
# date: 01/12/2020

from preprocessers.__preprocessor import Preprocessor

from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql import DataFrame


class TaskEventPreprocessor(Preprocessor):
    __instance__ = None
    __data_path = "data/preprocessors/task_event"
    __transformer__: PipelineModel

    def __init__(self):
        if TaskEventPreprocessor.__instance__ is None:
            TaskEventPreprocessor.__instance__ = self
        else:
            raise Exception("Cannot construct directly {}".format(self.__class__))

    @classmethod
    def get_instance(cls):
        if TaskEventPreprocessor.__instance__ is None:
            TaskEventPreprocessor()
        return TaskEventPreprocessor.__instance__

    def preprocess(self, input_df: DataFrame):
        df_selected = self.__remain_features(input_df)
        df_fitted = TaskEventPreprocessor.__transformer__.transform(df_selected)
        return df_fitted

    def fit(self, input_df: DataFrame):
        df_selected = self.__remain_features(input_df)
        stages = []
        vector_assembler = VectorAssembler(inputCols=df_selected.columns,
                                           outputCol="features")
        stages.append(vector_assembler)
        pipeline = Pipeline(stages=stages)
        TaskEventPreprocessor.__transformer__ = pipeline.fit(df_selected)
        try:
            TaskEventPreprocessor.__transformer__.save(self.__data_path)
        except Exception as e:
            TaskEventPreprocessor.__transformer__.write().overwrite().save(self.__data_path)

    def setup(self):
        TaskEventPreprocessor.__transformer__ = PipelineModel.read().load(self.__data_path)

    def __remain_features(self, input_df: DataFrame):
        df_selected = input_df.select('_c10', '_c11', '_c12')
        df_non_null = df_selected.dropna()
        for c in df_non_null.columns:
            df_non_null = df_non_null.withColumn(c, col(c).cast("float"))
        return df_non_null


if __name__ == '__main__':
    from loaders import TaskEventLoader

    path = "hdfs://localhost:9000/data/task-event"
    loader = TaskEventLoader()
    df = loader.load(path)
    preprocessor = TaskEventPreprocessor.get_instance()
    preprocessor.fit(df)
    new_df = preprocessor.preprocess(df)
    df.show(3)
    print("-------------------------------")
    new_df.show(3)
