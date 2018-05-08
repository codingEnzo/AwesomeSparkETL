# -*- coding: utf-8 -*-
from __future__ import print_function
from random import randint

from pyspark.sql import Row, SparkSession

from SparkETLCore.CityCore.Xuzhou import ProjectCore
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='spark_test'):
    return {
        "url":
        "jdbc:mysql://10.30.1.70:3307/{}?useUnicode=true&characterEncoding=utf8" \
        .format(db),
        "driver":
        "com.mysql.jdbc.Driver",
        "dbtable":
        "(SELECT * FROM ProjectInfoItem WHERE City = '{}') {}".format(
            city, tableName),
        "user":
        "root",
        "password":
        "gh001"
    }


def cleanFields(spark, row, methods, target, fields):
    row = row.asDict()
    row = Var.NiceDict(dictionary=row, field_list=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(spark, row)
    row = Row(**row)
    return row


def groupedWork(spark, grouped, methods, target, fields):
    for i, (num, group) in enumerate(grouped):
        df = spark.createDataFrame(group)
        df = df.rdd.map(lambda r: cleanFields(spark, r, methods, target, fields))
        df.select(fields).toDF().write().format("jdbc") \
                .options(
                    url="jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf8",
                    driver="com.mysql.jdbc.Driver",
                    dbtable="project_info_xuzhou",
                    user="root",
                    password="gh001") \
                .mode("append") \
                .save()
    return grouped


def main():
    appName = 'xuzhou'
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    projectArgs = kwarguments('ProjectInfoItem', '徐州')
    projectDF = spark.read \
                     .format("jdbc") \
                     .options(**projectArgs) \
                     .load() \
                     .fillna("")
    projectDF.createOrReplaceTempView("ProjectInfoItem")

    buildingArgs = kwarguments('BuildingInfoItem', '徐州')
    buildingDF = spark.read \
                     .format("jdbc") \
                     .options(**buildingArgs) \
                     .load() \
                     .fillna("")
    buildingDF.createOrReplaceTempView("BuildingInfoItem")

    houseArgs = kwarguments('HouseInfoItem', '徐州')
    houseDF = spark.read \
                     .format("jdbc") \
                     .options(**houseArgs) \
                     .load() \
                     .fillna("")
    houseDF.createOrReplaceTempView("HouseInfoItem")

    presellArgs = kwarguments('PresellInfoItem', '徐州')
    presellDF = spark.read \
                     .format("jdbc") \
                     .options(**presellArgs) \
                     .load() \
                     .fillna("")
    presellDF.createOrReplaceTempView("PresellInfoItem")

    # ProjectCore
    # ---
    projectDF.rdd \
             .map(lambda r: (randint(1, 8), [r])) \
             .reduceByKey(lambda x, y: x + y) \
             .map(lambda g: groupedWork(spark, g, ProjectCore.METHODS, ProjectCore, Var.PROJECT_FIELDS).count()

    return 0


if __name__ == "__main__":
    main()
