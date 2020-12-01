# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __task_event_trainer.py
# project name: ModelManager
# date: 01/12/2020
from pyspark.ml import Estimator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import GaussianMixture

from loaders import TaskEventDataLoader, DataLoader
from preprocessers import Preprocessor
from trainers.__trainer import Trainer


class TaskEventTrainer(Trainer):

    def _get_model_path(self) -> str:
        return "hdfs://localhost:9000/data/models/task-event"

    def _get_preprocessor(self) -> Preprocessor:
        cols = [col for col in self._get_cols()]
        return VectorAssembler(inputCols=cols,
                               outputCol="features")

    def _get_data_loader(self) -> DataLoader:
        return TaskEventDataLoader()

    def _get_data_path(self) -> str:
        return "hdfs://localhost:9000/data/task-event"

    def _create_estimator(self) -> Estimator:
        gmm = GaussianMixture().setK(3)
        return gmm

    def _get_cols(self) -> list:
        return ['_c10', '_c11', '_c12']
