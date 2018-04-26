# coding=utf-8
import json
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import sys
sys.path.append('/home/sun/AwesomeSparkETL/Src/SparkETLCore/Changzhou')
import ProjectCore
import HouseCore
import BuildingCore
import QuitCaseCore
import SupplyCaseCore
import DealCaseCore

ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')


def getData(cls, data):
    for method in cls.METHODS:
        func = getattr(cls, method)
        data = func(data)
    return data


def runFunc(func, sql, tosqlname):
    data = pd.read_sql(sql, ENGINE)
    SparkSession.builder.appName(func)
    spark = SparkSession.builder.appName(func).getOrCreate()
    dataDF = spark.createDataFrame(data)
    res = dataDF.rdd.map(lambda r: getData(func, r)).collect()
    rdd = spark.sparkContext.parallelize(res)
    pdf = rdd.toDF().toPandas()
    pdf.to_sql(tosqlname, MIRROR_ENGINE,
               if_exists='append', index=False)


quit_sql = "select * from HouseInfoItem where City='常州' and HouseStateLatest='已备案' and HouseState='未备案'"
runFunc(QuitCaseCore, quit_sql, quit_case_changzhou)
supply_sql = "select * from HouseInfoItem where City='常州' and (HouseStateLatest='未备案' or HouseStateLatest='') and HouseState='未备案'"
runFunc(SupplyCaseCore, supply_sql, quit_case_changzhou)
deal_sql = "select * from HouseInfoItem where City='常州' and HouseStateLatest='未备案' and HouseState='已备案'"
runFunc(DealCaseCore, deal_sql, quit_case_changzhou)
# select_sql = "select * from HouseInfoItem where City='常州'"
# data = pd.read_sql(select_sql, ENGINE)
# SparkSession.builder.appName('spark')
# spark = SparkSession.builder.appName('spark').getOrCreate()
# dataDF = spark.createDataFrame(data)
# res = dataDF.rdd.map(lambda r: getData(HouseCore, r)).collect()
# rdd = spark.sparkContext.parallelize(res)
# pdf = rdd.toDF().toPandas()
# pdf.to_sql('house_info_changzhou', MIRROR_ENGINE,
#            if_exists='replace', index=False)
