# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import findspark
from pyspark.sql import SparkSession
from model_manager import ModelManager

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    findspark.init("/home/quangkhanh/spark-3.0.0-preview2-bin-hadoop2.7")
    sparkSession = SparkSession.builder.appName("Train Model").master("local").getOrCreate()
    manager = ModelManager().run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
