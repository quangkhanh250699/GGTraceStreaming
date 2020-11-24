# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: __init__.py
# project name: LogProducer
# date: 24/11/2020

from config._config import Config
from config._connector_config import ConnectorConfig

__all__ = [
    'Config',
    'ConnectorConfig'
]