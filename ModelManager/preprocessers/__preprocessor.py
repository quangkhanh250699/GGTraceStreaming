# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __preprocessor.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod


class Preprocessor(metaclass=ABCMeta):

    @abstractmethod
    def preprocess(self, input_df):
        pass

    @abstractmethod
    def fit(self, input_df):
        pass

    @abstractmethod
    def setup(self):
        pass
