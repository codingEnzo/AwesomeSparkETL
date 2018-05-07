# -*- coding: utf-8 -*-
from __future__ import print_function
import sys

from pyspark.sql import Row, SparkSession, SQLContext

from SparkETLCore.CityCore.Xuzhou import ProjectCore


def main():
    appName = 'xuzhou'
    tableName = 'ProjectInfoItem'
    sc = SparkSession.builder.appName(appName).getOrCreate()
    ctx = SQLContext(sc)
    jdbcDF = ctx.read.format("jdbc").options(
        url=
        "jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf8",
        driver="com.mysql.jdbc.Driver",
        dbtable=
        "(SELECT * FROM ProjectInfoItem WHERE City = '徐州') ProjectInfoItem",
        user="root",
        password="gh001").load()

    jdbcDF = jdbcDF \
             .fillna("").rdd \
             .map(lambda x: cleanFields(x, ProjectCore.METHODS, ProjectCore)) \
             .toDF()
    jdbcDF.write.format("jdbc") \
                .options(
                    url="jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf8",
                    driver="com.mysql.jdbc.Driver",
                    dbtable="project_info_xuzhou",
                    user="root",
                    password="gh001") \
                .mode("append") \
                .save()
    return 0


def cleanFields(row, methods, target):
    row = row.asDict()
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


if __name__ == "__main__":
    main()
