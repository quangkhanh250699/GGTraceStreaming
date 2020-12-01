# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: model_manager.py
# project name: ModelManager
# date: 01/12/2020

from trainers import TaskEventTrainer


class ModelManager:
    __trainers__ = [
        TaskEventTrainer()
    ]

    @classmethod
    def run(cls):
        for trainer in cls.__trainers__:
            trainer.train()
