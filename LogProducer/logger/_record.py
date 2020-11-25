# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _record.py
# project name: LogProducer
# date: 24/11/2020

from abc import ABCMeta, abstractmethod
from typing import List


class Record(metaclass=ABCMeta):

    @abstractmethod
    def to_record_string(self, payload: str) -> str:
        pass

    def to_record_strings(self, payloads: List[str]) -> List[str]:
        return list(map(lambda x: self.to_record_string(x), payloads))


class TaskUsageRecord(Record):

    def to_record_string(self, payload: str) -> str:
        return payload
