import os
import sys
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from sqlalchemy import *
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import PermitCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import DealcaseCore
sys.path.append(os.path.dirname(os.getcwd()))

def cleanProject(sc):
    url = "jdbc:mysql://localhost:3306/citymap_pgy_xuzhou"
    jdbcDf=sc.read.format("jdbc").options(url="jdbc:mysql://10.30.1.70:3307/spark_test?charset=utf8",
                                           driver="com.mysql.jdbc.Driver",
                                           dbtable="(SELECT * FROM ProjectInfoItem WHERE City='合肥') tmp",user="root",
                                           password="gh001").load()
    # print(jdbcDf.printSchema())
    # print (jdbcDf.show())

def getData(data):
    for index,method in enumerate(ProjectCore.METHODS):
        if index ==0:
            func = getattr(PermitCore,method)
            data = func(data)
    return data
if __name__ == '__main__':
    MIRROR_ENGINE = create_engine(
        'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
    sc = SparkSession.builder.appName("master").getOrCreate()
    pd = pd.read_sql("SELECT * FROM ProjectInfoItem WHERE City='合肥'",con=MIRROR_ENGINE)
    sdf = sc.createDataFrame(pd)
    sdd =sdf.rdd
    # for i in method[:1]:
    #     b =sdd.map(getattr(ProjectCore,i)).collect()
    b =sdd.map(getattr(ProjectCore,'test')).collect()
    # kk = pd.DataFrame(b)
    b.to_csv('/home/yfwyj/AwesomeSparkETL/Src/aa.csv')
    # print (b[1])

    # print(sdd.take(1))

    # cleanProject(sc,host,user,pwd)
