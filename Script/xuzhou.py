# -*- coding: utf-8 -*-
from __future__ import print_function
from random import randint

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col

from SparkETLCore.CityCore.Xuzhou import ProjectCoreUDF
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='spark_test'):
    return {
        "url":
        "jdbc:mysql://10.30.1.70:3307/{}?useUnicode=true&characterEncoding=utf8" \
        .format(db),
        "driver":
        "com.mysql.jdbc.Driver",
        "dbtable":
        "(SELECT * FROM {tb} WHERE City = '{ct}') {tb}".format(
            ct=city, tb=tableName),
        "user":
        "root",
        "password":
        "gh001"
    }


def pdf_apply(pdf, target_method=None, target_table=None):
    ix = {
        'PresellInfoItem': target_method.get('PresellInfoItem')
        'HouseInfoItem': target_method.get('HouseInfoItem')
    }
    methods = ix[target_table]
    for i, m in enumerate(methods):
        pdf = m(pdf)
    return pdf


def groupedWork(grouped, methods, target, fields):
    for i, (num, group) in enumerate(grouped):
        spark = SparkSession.builder.appName("xuzhou").getOrCreate()
        df = spark.createDataFrame(group)
        df = df.rdd.map(lambda r: cleanFields(r, methods, target, fields))
        df.toDF().select(fields).write().format("jdbc") \
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
    # projectDF.createOrReplaceGlobalTempView("ProjectInfoItem")

    buildingArgs = kwarguments('BuildingInfoItem', '徐州')
    buildingDF = spark.read \
                     .format("jdbc") \
                     .options(**buildingArgs) \
                     .load() \
                     .fillna("")
    # buildingDF.createOrReplaceGlobalTempView("BuildingInfoItem")

    houseArgs = kwarguments('HouseInfoItem', '徐州')
    houseDF = spark.read \
                     .format("jdbc") \
                     .options(**houseArgs) \
                     .load() \
                     .fillna("")
    # houseDF.createOrReplaceGlobalTempView("HouseInfoItem")

    presellArgs = kwarguments('PresellInfoItem', '徐州')
    presellDF = spark.read \
                     .format("jdbc") \
                     .options(**presellArgs) \
                     .load() \
                     .fillna("")
    # presellDF.createOrReplaceGlobalTempView("PresellInfoItem")

    # ProjectCore
    x = projectDF.alias('x')
    y = presellDF.alias('y').groupby('ProjectUUID').agg()
    z = houseDF.alias('z').groupby('ProjectUUID').agg()

    # df_xy = x.join(y, col('x.ProjectUUID') == col('y.ProjectUUID')) \
    #       .select(['x.*', 'y.ExtraJson', 'y.LandUse', 'y.PresalePermitNumber'])

    return 0


if __name__ == "__main__":
    main()
