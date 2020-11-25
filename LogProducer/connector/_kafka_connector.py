# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _kafka_connector.py
# project name: LogProducer
# date: 24/11/2020

from kafka import KafkaProducer
from typing import List

from connector._connector import Connector
from config import ConnectorConfig


class KafkaConnector(Connector):
    __producer: KafkaProducer
    __topic: str

    def __init__(self, config: ConnectorConfig):
        self.__producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKERS,
            client_id=config.CLIENT_ID,
            max_block_ms=config.MAX_BLOCK_MS
        )
        self.__topic = config.topic

    @classmethod
    def getNewInstance(cls, config: ConnectorConfig):
        return cls(config)

    def submit(self, payload: List[str]):
        try:
            for data in payload:
                metadata = self.__producer.send(self.__topic, data.encode()).get()
        except KeyboardInterrupt:
            pass
