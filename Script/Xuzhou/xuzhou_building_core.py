# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from SparkETLCore.CityCore.Xuzhou import BuildingCoreUDF
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='spark_test'):
    return {
        "url":
        "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8" \
        .format(db),
        "driver":
        "com.mysql.jdbc.Driver",
        "dbtable":
        "(SELECT * FROM {tb} WHERE City = '{ct}' ORDER BY RecordTime DESC) {tb}".format(
            ct=city, tb=tableName),
        "user":
        "root",
        "password":
        "yunfangdata"
    }


def main():
    appName = 'xuzhou_building'
    spark = SparkSession.builder.appName(appName).config('spark.cores.max', 4).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    buildingArgs = kwarguments('BuildingInfoItem', '徐州')
    buildingDF = spark.read \
                     .format("jdbc") \
                     .options(**buildingArgs) \
                     .load() \
                     .fillna("")

    houseArgs = kwarguments('HouseInfoItem', '徐州')
    houseDF = spark.read \
                     .format("jdbc") \
                     .options(**houseArgs) \
                     .load() \
                     .fillna("")

    # BuildingCore Block --->
    x = buildingDF.alias('x')
    y = houseDF.alias('y')

    # 1. 字段清洗 + 提取
    x = x.withColumn('RecordTime',
                     BuildingCoreUDF.record_time_clean(x.RecordTime))

    # 2. 分组 + 聚合
    y = y.groupBy("BuildingUUID").agg(
        F.collect_list("UnitName").alias("UnitName"))

    # 3. 细节运算
    y = y.withColumn("UnitName", BuildingCoreUDF.unit_name_apply(y.UnitName))

    # 4. 联合入库
    x = x.drop(
        *[c for c in x.columns if (c in y.columns) and (c != 'BuildingUUID')])
    df = x.join(y, x.BuildingUUID == y.BuildingUUID, 'left')
    columns = df.columns
    for i, c in enumerate(Var.PROJECT_FIELDS):
        if c not in columns:
            df = df.withColumn(c, F.lit(""))
    name_list = set(Var.BUILDING_FIELDS) - set(['BuildingUUID'])
    df = df.dropDuplicates(['BuildingID'])
    df.select('x.BuildingUUID', *name_list).write.format("jdbc") \
        .options(
            url="jdbc:mysql://10.30.1.7:3306/mirror?useUnicode=true&characterEncoding=utf8",
            driver="com.mysql.jdbc.Driver",
            dbtable='building_info_xuzhou',
            user="root",
            password="yunfangdata") \
        .mode("append") \
        .save()
    # <--- BuildingCore End Block

    return 0


if __name__ == "__main__":
    main()
