# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from SparkETLCore.CityCore.Xuzhou import ProjectCoreUDF
from SparkETLCore.Utils import Var


def kwarguments(tableName, city, db='spark_test'):
    return {
        "url":
        "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8" \
        .format(db),
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
    y = presellDF.alias('y')
    z = houseDF.alias('z')

    # 1. 字段清洗 + 提取
    x = x.withColumn('RecordTime',
                     ProjectCoreUDF.record_time_clean(x.RecordTime))

    y = y.withColumn('LandUse', ProjectCoreUDF.land_use_clean(y.LandUse))
    y = y.withColumn('LssueDate', ProjectCoreUDF.lssue_date_clean(y.LssueDate))
    y = y.withColumn('PresalePermitNumber',
                     ProjectCoreUDF.presale_permit_number_clean(
                         y.PresalePermitNumber))
    y = y.withColumn('RegionName',
                     ProjectCoreUDF.region_name_extract(y.ExtraJson))
    y = y.withColumn('LandUsePermit',
                     ProjectCoreUDF.land_use_permit_extract(y.ExtraJson))
    y = y.withColumn('ConstructionPermitNumber',
                     ProjectCoreUDF.construction_permit_number_extract(
                         y.ExtraJson))
    y = y.withColumn('CertificateOfUseOfStateOwnedLand',
                     ProjectCoreUDF.cert_state_land_extract(y.ExtraJson))

    # 2. 分组 + 聚合
    y = y.groupBy("ProjectUUID").agg(
        F.collect_list("LandUse").alias('LandUse'),
        F.collect_list("LssueDate").alias('LssueDate'),
        F.collect_list("PresalePermitNumber").alias('PresalePermitNumber'),
        F.collect_list("RegionName").alias("RegionName"),
        F.collect_list("LandUsePermit").alias("LandUsePermit"),
        F.collect_list("ConstructionPermitNumber").alias(
            "ConstructionPermitNumber"),
        F.collect_list("CertificateOfUseOfStateOwnedLand").alias(
            "CertificateOfUseOfStateOwnedLand"))

    z = z.groupBy("ProjectUUID").agg(
        F.collect_list("HouseUseType").alias("HouseUseType"))

    # 3. 细节运算
    y = y.withColumn("LandUse", ProjectCoreUDF.land_use_apply(y.LandUse))
    y = y.withColumn("LssueDate", ProjectCoreUDF.lssue_date_apply(y.LssueDate))
    y = y.withColumn("PresalePermitNumber",
                     ProjectCoreUDF.presale_permit_number_apply(
                         y.PresalePermitNumber))
    y = y.withColumn("RegionName",
                     ProjectCoreUDF.region_name_apply(y.RegionName))
    y = y.withColumn("LandUsePermit",
                     ProjectCoreUDF.land_use_permit_apply(y.LandUsePermit))
    y = y.withColumn("ConstructionPermitNumber",
                     ProjectCoreUDF.construction_permit_number_apply(
                         y.ConstructionPermitNumber))
    y = y.withColumn("CertificateOfUseOfStateOwnedLand",
                     ProjectCoreUDF.cert_state_land_apply(
                         y.CertificateOfUseOfStateOwnedLand))

    z = z.withCoulumn("HouseUseType",
                      ProjectCoreUDF.house_use_type_apply(z.HouseUseType))

    # 4. 联合入库
    x = x.drop([c for c in x.columns if c in y.columns + z.columns])
    df = x.join(y, x.ProjectUUID == y.ProjectUUID, 'left') \
          .join(z, x.ProjectUUID == z.ProjectUUID, 'left')

    # <--- ProjectCore End Block

    return 0


if __name__ == "__main__":
    main()
