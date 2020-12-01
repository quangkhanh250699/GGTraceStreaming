# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __loader.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod


class Loader(metaclass=ABCMeta):

    @abstractmethod
    def load(self, path: str):
        pass
