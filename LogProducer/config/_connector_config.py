# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _connector_config.py
# project name: LogProducer
# date: 24/11/2020

import msgpack

from config._config import Config


class ConnectorConfig(Config):
    KAFKA_BROKERS = "localhost:9092"
    MESSAGE_COUNT = 1000
    CLIENT_ID = "client1"
    GROUP_ID_CONFIG = "consumerGroup10"
    MAX_NO_MESSAGE_FOUND_COUNT = 100
    MAX_POLL_RECORDS = 1
    MAX_BLOCK_MS = 30000

    topic: str

    def __init__(self, topic: str):
        self.__topic = topic

    @classmethod
    def value_serializer(value: str):
        msgpack.dumps(value)

    @property
    def topic(self):
        return self.__topic
