# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __trainer.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod
from pyspark.ml import Estimator, Pipeline
from pyspark.sql.functions import col

from preprocessers import Preprocessor
from loaders import DataLoader, ModelLoader


class Trainer(metaclass=ABCMeta):

    @abstractmethod
    def _get_model_path(self) -> str:
        pass

    @abstractmethod
    def _get_preprocessor(self) -> Preprocessor:
        pass

    @abstractmethod
    def _get_data_loader(self) -> DataLoader:
        pass

    @abstractmethod
    def _get_data_path(self) -> str:
        pass

    @abstractmethod
    def _create_estimator(self) -> Estimator:
        pass

    @abstractmethod
    def _get_cols(self) -> list:
        pass

    def train(self):
        input_df = self._get_data_loader().load(self._get_data_path())
        cols = self._get_cols()
        df_selected = input_df.select(cols)
        print(df_selected)
        for c in cols:
            input_df = input_df.withColumn(c, col(c).cast("float"))
        preprocessor = self._get_preprocessor()
        estimator = self._create_estimator()

        stages = [preprocessor, estimator]
        pipeline = Pipeline(stages=stages)
        print("-------------------------- start training ---------------------")
        model = pipeline.fit(input_df)
        print("-------------------------- end training ------------------------")
        try:
            model.save(self._get_model_path)
        except Exception as e:
            model.write().overwrite().save(self._get_model_path())
