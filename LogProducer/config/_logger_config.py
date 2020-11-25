# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _logger_config.py
# project name: LogProducer
# date: 24/11/2020

from config._config import Config

from typing import List


class LoggerConfig(Config):

    __name: str
    __info_logged: List[str]

    def __init__(self, name: str, info_logged: List[str]):
        self.__name = name
        self.__info_logged = info_logged

    @property
    def name(self):
        return self.__name
    
    @property
    def info_logged(self):
        return self.__info_logged
