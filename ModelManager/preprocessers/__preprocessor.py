# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __preprocessor.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod

from pyspark.ml import Estimator


class Preprocessor(metaclass=ABCMeta):

    @abstractmethod
    def get_estimator(self) -> Estimator:
        pass
