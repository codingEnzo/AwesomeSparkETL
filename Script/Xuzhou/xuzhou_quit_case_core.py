# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from SparkETLCore.CityCore.Xuzhou import QuitCaseCoreUDF
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
    appName = 'xuzhou_quit_case'
    spark = SparkSession.builder.appName(appName).config('spark.cores.max',
                                                         4).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

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
                   .fillna("").filter("HouseStateLatest = '已备案' AND HouseState IN ('自留', '可销售')")

    presellArgs = kwarguments('PresellInfoItem', '徐州')
    presellDF = spark.read \
                     .format("jdbc") \
                     .options(**presellArgs) \
                     .load() \
                     .fillna("")

    # ProjectCore Block --->
    x = projectDF.alias('x')
    y = houseDF.alias('y')
    z = presellDF.alias('z')

    # 1. 字段清洗 + 提取
    y = y.withColumn("City", QuitCaseCoreUDF.city_clean(y.City))
    y = y.withColumn("BuildingStructure",
                     QuitCaseCoreUDF.building_structure_clean(
                         y.BuildingStructure))
    y = y.withColumn("PriceType", F.lit("项目均价"))
    y = y.withColumn("State", F.lit("明确退房"))
    y = y.withColumn("RecordTime",
                     QuitCaseCoreUDF.record_time_clean(y.RecordTime))

    z = z.withColumn('RegionName',
                     QuitCaseCoreUDF.region_name_extract(z.ExtraJson))
    z = z.withColumn('PresalePermitNumber',
                     QuitCaseCoreUDF.presale_permit_number_clean(
                         z.PresalePermitNumber))

    # 2. 分组 + 聚合
    x = x.groupBy("ProjectUUID").agg(
        F.collect_list("ProjectAddress").alias("Address"),
        F.collect_list("DistrictName").alias("DistrictName"),
        F.collect_list("AveragePrice").alias("Price"),
    )

    z = z.groupBy("ProjectUUID").agg(
        F.collect_list("RegionName").alias("RegionName"),
        F.first("PresalePermitNumber").alias("PresalePermitNumber"),
    )

    # 3. 细节运算
    x = x.withColumn("Address", QuitCaseCoreUDF.address_apply(x.Address))
    x = x.withColumn("DistrictName",
                     QuitCaseCoreUDF.district_name_apply(x.DistrictName))
    x = x.withColumn("Price", QuitCaseCoreUDF.price_apply(x.Price))

    z = z.withColumn("RegionName",
                     QuitCaseCoreUDF.region_name_apply(z.RegionName))
    # 4. 联合入库
    y = y.drop(
        *[c for c in y.columns if (c in x.columns) and (c != 'ProjectUUID')])
    df = y.join(x, y.ProjectUUID == x.ProjectUUID, 'left') \
          .join(z, 'ProjectUUID', 'left')
    columns = df.columns
    for i, c in enumerate(Var.QUIT_FIELDS):
        if c not in columns:
            df = df.withColumn(c, F.lit(""))
    name_list = set(Var.QUIT_FIELDS) - set(['ProjectUUID'])
    df = df.select('y.ProjectUUID', *name_list)
    try:
        originArgs = kwarguments(
            'quit_case_info_xuzhou', '徐州', db='achievement')
        originDF = spark.read \
                        .format("jdbc") \
                        .options(**originArgs) \
                        .load() \
                        .fillna("")
        df = df.unionByName(originDF)
    except Exception as e:
        import traceback
        traceback.print_exc()

    df = df.dropDuplicates(subset=['HouseID'])
    df.write.format("jdbc") \
        .options(
            url="jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
            driver="com.mysql.jdbc.Driver",
            dbtable='quit_case_xuzhou',
            user="root",
            password="yunfangdata") \
        .mode("overwrite") \
        .save()
    # <--- HouseCore End Block

    return 0


if __name__ == "__main__":
    main()
