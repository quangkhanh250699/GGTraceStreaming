# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: timer.py
# project name: LogProducer
# date: 24/11/2020


class Timer:

    __instance__ = None
    __clock__: float
    __interval_time__: float

    def __init__(self, interval_time):
        if Timer.__instance__ is None:
            Timer.__clock__ = 0
            Timer.__interval_time__ = interval_time
            Timer.__instance__ = self
        else:
            raise Exception("Cannot create another instance of Time!")

    @classmethod
    def get_instance(cls, interval_time=5):
        if cls.__instance__ is None:
            Timer(interval_time)
        return cls.__instance__

    @classmethod
    def clock(cls):
        cls.__clock__ += cls.__interval_time__
        return cls.__clock__

    @classmethod
    def check(cls):
        return cls.__clock__


if __name__ == '__main__':
    Timer(10)

    for i in range(10):
        print(Timer.clock())
