# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __task_event_trainer.py
# project name: ModelManager
# date: 01/12/2020
from pyspark.ml import Estimator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import Imputer
from pyspark.ml.pipeline import Pipeline

from loaders import TaskEventDataLoader, DataLoader
from trainers.__trainer import Trainer


class TaskEventTrainer(Trainer):

    def _get_model_path(self) -> str:
        return "hdfs://localhost:9000/data/models/task-event"

    def _get_preprocessor(self):
        cols = [col for col in self._get_cols()]
        imputer = Imputer(inputCols=cols, outputCols=cols)
        vector_assembler =  VectorAssembler(inputCols=cols,
                                            outputCol="features").setHandleInvalid("keep")
        return Pipeline(stages=[imputer, vector_assembler])

    def _get_data_loader(self) -> DataLoader:
        return TaskEventDataLoader()

    def _get_data_path(self) -> str:
        return "hdfs://localhost:9000/data/task-event"

    def _create_estimator(self) -> Estimator:
        model = GaussianMixture().setK(3).setSeed(1)
        return model

    def _get_cols(self) -> list:
        return ['_c10', '_c11', '_c12']
