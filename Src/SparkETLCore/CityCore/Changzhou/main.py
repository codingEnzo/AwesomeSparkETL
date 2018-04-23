# coding=utf-8
import json
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import ProjectCore

ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')


def getData(cls, data):
    for method in cls.METHODS:
        func = getattr(cls, method)
        data = func(data)
    return data


select_sql = "select * from ProjectInfoItem where City='常州'"
project_data = pd.read_sql(select_sql, ENGINE)
SparkSession.builder.appName('spark')
spark = SparkSession.builder.appName('spark').getOrCreate()
data = spark.createDataFrame(project_data)
res = data.rdd.map(lambda r: getData(ProjectCore, r)).collect()
result = pd.DataFrame(res)
result.to_sql('project_info_changzhou', MIRROR_ENGINE)
