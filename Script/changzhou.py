# -*- coding: utf-8 -*-
from __future__ import print_function
from random import randint

from pyspark.sql import Row, SparkSession

from SparkETLCore.CityCore.Changzhou import HouseCore, BuildingCore, ProjectCore, SupplyCaseCore, QuitCaseCore, DealCaseCore

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


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    row = Var.NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def groupedWork(data,fields, tableName):
    df = data
    df.toDF().select(fields).write.format("jdbc") \
        .options(
        url="jdbc:mysql://10.30.1.7:3306/spark_mirror?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true",
        driver="com.mysql.jdbc.Driver",
        dbtable=tableName,
        user="root",
        password="yunfangdata") \
        .mode("append") \
        .save()
    return df


def main():
    appName = 'changzhou'
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    projectArgs = kwarguments('ProjectInfoItem', '常州')
    projectDF = spark.read \
        .format("jdbc") \
        .options(**projectArgs) \
        .load() \
        .fillna("")
    projectDF.createOrReplaceTempView("ProjectInfoItem")

    buildingArgs = kwarguments('BuildingInfoItem', '常州')
    buildingDF = spark.read \
        .format("jdbc") \
        .options(**buildingArgs) \
        .load() \
        .fillna("")
    projectDF.createOrReplaceTempView("BuildingInfoItem")

    houseArgs = kwarguments('HouseInfoItem', '常州')
    houseDF = spark.read \
        .format("jdbc") \
        .options(**houseArgs) \
        .load() \
        .fillna("")
    projectDF.createOrReplaceTempView("HouseInfoItem")

    # presellArgs = kwarguments('PresellInfoItem', '常州')
    # presellDF = spark.read \
    #     .format("jdbc") \
    #     .options(**presellArgs) \
    #     .load() \
    #     .fillna("")

    projectDF.rdd.map(lambda r: cleanFields(r,ProjectCore.METHODS, ProjectCore, Var.PROJECT_FIELDS)) \
        .map(lambda g: groupedWork(g,Var.PROJECT_FIELDS,'project_info_changzhou')).count()
    buildingDF.rdd \
        .map(lambda r: (randint(1, 8), [r])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda g: groupedWork(spark, g, BuildingCore.METHODS, BuildingCore, Var.BUILDING_FIELDS)).count()
    houseDF.rdd \
        .map(lambda r: (randint(1, 8), [r])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda g: groupedWork(spark, g, HouseCore.METHODS, HouseCore, Var.HOUSE_FIELDS)).count()

    # houseDF.rdd \
    #     .map(lambda r: (randint(1, 8), [r])) \
    #     .reduceByKey(lambda x, y: x + y) \
    #     .map(lambda g: groupedWork(spark, g, SupplyCaseCore.METHODS, SupplyCaseCore, Var.HOUSE_FIELDS)).count()
    # houseDF.rdd \
    #     .map(lambda r: (randint(1, 8), [r])) \
    #     .reduceByKey(lambda x, y: x + y) \
    #     .map(lambda g: groupedWork(spark, g, QuitCaseCore.METHODS, QuitCaseCore, Var.HOUSE_FIELDS)).count()
    # houseDF.rdd \
    #     .map(lambda r: (randint(1, 8), [r])) \
    #     .reduceByKey(lambda x, y: x + y) \
    #     .map(lambda g: groupedWork(spark, g, DealCaseCore.METHODS, DealCaseCore, Var.HOUSE_FIELDS)).count()
    return 0


if __name__ == "__main__":
    main()
