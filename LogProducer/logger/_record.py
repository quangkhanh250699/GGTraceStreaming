# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _record.py
# project name: LogProducer
# date: 24/11/2020

from abc import ABCMeta, abstractmethod
from typing import List
import json


class Record(metaclass=ABCMeta):

    @abstractmethod
    def to_record_string(self, payload: str) -> str:
        pass

    def to_record_strings(self, payloads: List[str]) -> List[str]:
        return list(map(lambda x: self.to_record_string(x), payloads))


class TaskUsageRecord(Record):
    fields = [
        'id',
        'start_time',
        'end_time',
        'job_id',
        'task_index',
        'machine_id',
        'cpu_rate',
        'canonical_memory_usage',
        'assigned_memory_usage',
        'unmapped_page_cache',
        'total_page_cache',
        'maximum_memory_usage',
        'disk_io_time',
        'logical_diskspace_usage',
        'max_cpu_rate',
        'max_disk_io_time',
        'cycles_per_instruction',
        'memo_accesses_per_instruction',
        'sample_portion',
        'aggregation_type',
        'sample_cpu_usage'
    ]

    def to_record_string(self, payload: str) -> str:
        return payload

    def to_record_json(self, payload: str) -> str:
        values = payload.split(',')
        for i in range(6):
            try:
                values[i] = int(values[i])
            except ValueError:
                values[i] = None
        for i in range(6, len(values)):
            try:
                values[i] = float(values[i])
            except ValueError:
                values[i] = None
        payload_dict = {k: v for k, v in zip(self.fields, values)}
        payload_dict['aggregation_type'] = int(payload_dict['aggregation_type'])
        return json.dumps(payload_dict)


class TaskEventRecord(Record):
    fields = [
        'id',
        'time',
        'missing_info',
        'job_id',
        'task_index',
        'machine_id',
        'event_type',
        'user',
        'scheduling_class',
        'priority',
        'cpu_request',
        'memory_request',
        'disk_space_request',
        'different_machine_restriction'
    ]

    def to_record_string(self, payload: str) -> str:
        return payload

    def to_record_json(self, payload: str) -> str:
        values = payload.split(',')
        for i in [0, 1, 2, 3, 4, 5, 6, 8, 9, 13]:
            try:
                values[i] = int(values[i])
            except ValueError:
                values[i] = None
        for i in [10, 11, 12]:
            try:
                values[i] = float(values[i])
            except ValueError:
                values[i] = None
        payload_dict = {k: v for k, v in zip(self.fields, values)}
        return json.dumps(payload_dict)
