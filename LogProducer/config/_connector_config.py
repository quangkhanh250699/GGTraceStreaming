# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _connector_config.py
# project name: LogProducer
# date: 24/11/2020

import msgpack

from config._config import Config


class ConnectorConfig(Config):

    KAFKA_BROKERS = "kafka:9093"
    MESSAGE_COUNT = 1000
    CLIENT_ID = "client1"
    GROUP_ID_CONFIG = "consumerGroup10"
    MAX_NO_MESSAGE_FOUND_COUNT = 100
    MAX_POLL_RECORDS = 1
    MAX_BLOCK_MS = 30000
    AUTO_CREATE_TOPICS_ENABLE = True

    __name: str
    __topic: str

    def __init__(self, name: str, topic: str):
        self.__name = name
        self.__topic = topic

    @property
    def name(self) -> str:
        return self.__name

    @property
    def topic(self):
        return self.__topic

    @classmethod
    def value_serializer(cls, value: str):
        msgpack.dumps(value)
