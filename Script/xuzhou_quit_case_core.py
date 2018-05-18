# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

from SparkETLCore.CityCore.Xuzhou import DealCaseCoreUDF
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='spark_test'):
    return {
        "url":
        "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8".
        format(db),
        "driver":
        "com.mysql.jdbc.Driver",
        "dbtable":
        "(SELECT * FROM {tb} WHERE City = '{ct}') {tb}".format(
            ct=city, tb=tableName),
        "user":
        "root",
        "password":
        "yunfangdata"
    }


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

    houseArgs = kwarguments('HouseInfoItem', '徐州')
    houseDF = spark.read \
                     .format("jdbc") \
                     .options(**houseArgs) \
                     .load() \
                     .fillna("")

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
    y = y.withColumn("City", DealCaseCoreUDF.city_clean(y.City))
    y = y.withColumn("BuildingStructure",
                     DealCaseCoreUDF.building_structure_clean(
                         y.BuildingStructure))
    y = y.withColumn("PriceType", lit("项目均价"))

    z = z.withColumn('RegionName',
                     DealCaseCoreUDF.region_name_extract(z.ExtraJson))
    z = z.withColumn('PresalePermitNumber',
                     DealCaseCoreUDF.presale_permit_number_clean(
                         z.PresalePermitNumber))

    # 2. 分组 + 聚合
    x = x.groupBy("ProjectUUID").agg(
        F.collect_list("ProjectAddress").alias("Address"),
        F.collect_list("DistrictName").alias("DistrictName"),
    )

    z = z.groupBy("ProjectUUID").agg(
        F.collect_list("RegionName").alias("RegionName"),
        F.first("PresalePermitNumber").alias("PresalePermitNumber"),
    )

    # 3. 细节运算
    x = x.withColumn("Address", DealCaseCoreUDF.address_apply(x.Address))
    x = x.withColumn("DistrictName",
                     DealCaseCoreUDF.district_name_apply(x.DistrictName))

    z = z.withColumn("RegionName",
                     DealCaseCoreUDF.region_name_apply(z.RegionName))
    # 4. 联合入库
    y = y.drop(
        *[c for c in y.columns if (c in x.columns) and (c != 'ProjectUUID')])
    df = y.join(x, y.ProjectUUID == x.ProjectUUID, 'left') \
          .join(z, 'ProjectUUID', 'left')
    columns = df.columns
    for i, c in enumerate(Var.DEAL_FIELDS):
        if c not in columns:
            df = df.withColumn(c, lit(""))
    name_list = set(Var.DEAL_FIELDS) - set(['ProjectUUID'])
    df = df.dropDuplicates(subset=['HouseUUID'])
    df.select('y.ProjectUUID', *name_list).write.format("jdbc") \
        .options(
            url="jdbc:mysql://10.30.1.7:3306/mirror?useUnicode=true&characterEncoding=utf8",
            driver="com.mysql.jdbc.Driver",
            dbtable='deal_case_info_xuzhou',
            user="root",
            password="yunfangdata") \
        .mode("append") \
        .save()
    # <--- HouseCore End Block

    return 0


if __name__ == "__main__":
    main()
