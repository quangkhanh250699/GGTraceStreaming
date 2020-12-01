# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __model.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod


class Model(metaclass=ABCMeta):

    @abstractmethod
    def predict(self, input_df):
        pass
