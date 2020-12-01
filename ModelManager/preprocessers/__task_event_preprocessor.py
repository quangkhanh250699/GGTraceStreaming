# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __task_event_preprocessor.py
# project name: ModelManager
# date: 01/12/2020
from pyspark.ml.base import Estimator

from preprocessers.__preprocessor import Preprocessor
from loaders import ModelLoader

from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql import DataFrame


class TaskEventPreprocessor(Preprocessor):

    def __init__(self):
        pass

    def get_estimator(self, input_df) -> Estimator:
        df_selected = self.__remain_features(input_df)
        stages = []
        vector_assembler = VectorAssembler(inputCols=df_selected.columns,
                                           outputCol="features")
        stages.append(vector_assembler)
        estimator = Pipeline(stages=stages)
        return estimator

    def __remain_features(self, input_df: DataFrame):
        df_selected = input_df.select('_c10', '_c11', '_c12')
        df_non_null = df_selected.dropna()
        for c in df_non_null.columns:
            df_non_null = df_non_null.withColumn(c, col(c).cast("float"))
        return df_non_null

