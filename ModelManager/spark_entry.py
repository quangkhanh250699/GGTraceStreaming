# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: spark_entry.py
# project name: ModelManager
# date: 01/12/2020

import findspark

findspark.init('/home/quangkhanh/spark-3.0.0-preview2-bin-hadoop2.7')

from pyspark.sql import SparkSession

appName = "LogAnalyst"
master = "local"

sparkSession = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()
