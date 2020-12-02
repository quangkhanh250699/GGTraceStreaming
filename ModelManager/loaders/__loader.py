# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __loader.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod

from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame


class DataLoader(metaclass=ABCMeta):

    @abstractmethod
    def load(self, path: str) -> DataFrame:
        pass


class ModelLoader():
    @classmethod
    def load_pipeline_model(cls, model_path: str) -> PipelineModel:
        return PipelineModel.read().load(model_path)
