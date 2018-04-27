# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, SQLContext

# from ..SparkETLCore.CityCore.Xuzhou import ProjectCore


def main():
    appName = __name__
    # url = 'jdbc:mysql://10.30.1.70:3307/spark_test?user=root&passwd=gh001'
    sc = SparkSession.builder.appName(appName).getOrCreate()
    ctx = SQLContext(sc)
    jdbcDF = ctx.read.format("jdbc") \
        .options(
            url='jdbc:mysql://10.30.1.70:3307/spark_test?user=root&passwd=gh001',
            driver="com.mysql.jdbc.Driver",
            dbtable="SELECT * FROM ProjectInfoItem WHERE City='徐州'") \
        .load()
    jdbcDF.printSchema()

    return 0
