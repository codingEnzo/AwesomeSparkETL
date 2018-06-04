# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from SparkETLCore.CityCore.Xuzhou import HouseCoreUDF
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='naive'):
    if db != 'achievement':
        return {
            "url":
            "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8"
            .format(db),
            "driver":
            "com.mysql.jdbc.Driver",
            "dbtable":
            "(SELECT * FROM {tb} WHERE City = '{ct}' ORDER BY RecordTime DESC) {tb}".
            format(ct=city, tb=tableName),
            "user":
            "root",
            "password":
            "yunfangdata"
        }
    else:
        return {
            "url":
            "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8"
            .format(db),
            "driver":
            "com.mysql.jdbc.Driver",
            "dbtable":
            "(SELECT * FROM {tb}) {tb}".format(tb=tableName),
            "user":
            "root",
            "password":
            "yunfangdata"
        }


def main():
    appName = 'xuzhou_house'
    spark = SparkSession.builder.appName(appName).config('spark.cores.max',
                                                         4).getOrCreate()
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # spark.conf.set("spark.network.timeout", "800s")

    projectArgs = kwarguments('ProjectInfoItem', '徐州')
    projectDF = spark.read \
                     .format("jdbc") \
                     .options(**projectArgs) \
                     .load() \
                     .fillna("")

    houseArgs = kwarguments('HouseInfoItem', '徐州')
    houseDF = spark.read \
                     .format("jdbc") \
                     .options(**houseArgs) \
                     .load() \
                     .fillna("")

    # HouseCore Block --->
    x = projectDF.alias('x')
    y = houseDF.alias('y')

    # 1. 字段清洗 + 提取
    y = y.withColumn("City", HouseCoreUDF.city_clean(y.City))
    y = y.withColumn("BuildingStructure",
                     HouseCoreUDF.building_structure_clean(
                         y.BuildingStructure))
    y = y.withColumn("RecordTime", HouseCoreUDF.record_time_clean(
        y.RecordTime))

    # 2. 分组 + 聚合
    x = x.groupBy("ProjectUUID").agg(
        F.collect_list("ProjectAddress").alias("Address"),
        F.collect_list("DistrictName").alias("DistrictName"),
    )

    # 3. 细节运算
    x = x.withColumn("Address", HouseCoreUDF.address_apply(x.Address))
    x = x.withColumn("DistrictName",
                     HouseCoreUDF.district_name_apply(x.DistrictName))

    # 4. 联合入库
    y = y.drop(
        *[c for c in y.columns if (c in x.columns) and (c != 'ProjectUUID')])
    df = y.join(x, y.ProjectUUID == x.ProjectUUID, 'left')
    columns = df.columns
    for i, c in enumerate(Var.HOUSE_FIELDS):
        if c not in columns:
            df = df.withColumn(c, F.lit(""))
    name_list = set(Var.HOUSE_FIELDS) - set(['ProjectUUID'])
    df = df.select('y.ProjectUUID', *name_list)
    try:
        originArgs = kwarguments('house_info_xuzhou', '徐州', db='achievement')
        originDF = spark.read \
                     .format("jdbc") \
                     .options(**originArgs) \
                     .load() \
                     .fillna("")
        df = df.unionByName(originDF)
    except Exception as e:
        import traceback
        traceback.print_exc()
    df = df.dropDuplicates(['HouseID'])
    df.write.format("jdbc") \
        .options(
            url="jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
            driver="com.mysql.jdbc.Driver",
            dbtable='house_info_xuzhou',
            user="root",
            password="yunfangdata") \
        .mode("overwrite") \
        .save()
    # <--- HouseCore End Block

    return 0


if __name__ == "__main__":
    main()
