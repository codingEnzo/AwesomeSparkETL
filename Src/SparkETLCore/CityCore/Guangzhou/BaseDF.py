# coding=utf-8
from __future__ import division
import sys
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL Hive integration example")\
    .config('spark.cores.max', 8)\
    .config('spark.sql.execution.arrow.enabled', "true")\
    .getOrCreate()

projectDF = spark.read.format('jdbc').options(
    url='jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf8',
    driver='com.mysql.jdbc.Driver',
    dbtable='(SELECT * FROM(SELECT * FROM ProjectInfoItem WHERE city="广州" ORDER BY RecordTime DESC) AS col Group BY ProjectUUID) ProjectInfoItem',
    user='root',
    password='gh001').load()
projectDF.registerTempTable("projectinfoitem")


buildingDF = spark.read.format('jdbc').options(
    url='jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf8',
    driver='com.mysql.jdbc.Driver',
    dbtable='(SELECT * FROM(SELECT * FROM BuildingInfoItem WHERE city="广州" ORDER BY RecordTime DESC) AS col Group BY BuildingUUID) BuildingInfoItem',
    user='root',
    password='gh001').load()
buildingDF.registerTempTable("buildinginfoitem")


houseDF = spark.read.format('jdbc').options(
    url='jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf8',
    driver='com.mysql.jdbc.Driver',
    dbtable='(SELECT * FROM(SELECT * FROM HouseInfoItem WHERE city="广州" ORDER BY RecordTime DESC) AS col Group BY HouseUUID) HouseInfoItem',
    user='root',
    password='gh001').load()
houseDF.registerTempTable("houseinfoitem")
