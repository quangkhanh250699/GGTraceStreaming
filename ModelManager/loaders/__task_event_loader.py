# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __task_event_loader.py
# project name: ModelManager
# date: 01/12/2020

from loaders import Loader
from spark_entry import sparkSession


class TaskEventLoader(Loader):

    def load(self, path: str):
        return sparkSession.read\
            .option("header", "false")\
            .csv(path)


if __name__ == '__main__':
    path = "hdfs://localhost:9000/data/task-event"
    t = TaskEventLoader()
    t.load(path).show(10)