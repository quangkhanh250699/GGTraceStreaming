# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __trainer.py
# project name: ModelManager
# date: 01/12/2020

from abc import ABCMeta, abstractmethod

from models import Model


class Trainer(metaclass=ABCMeta):

    @abstractmethod
    def train(self, model: Model):
        pass
