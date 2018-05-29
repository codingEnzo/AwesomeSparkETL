# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from SparkETLCore.CityCore.Xuzhou import PresellCoreUDF
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
    appName = 'xuzhou_presell'
    spark = SparkSession.builder.appName(appName).config('spark.cores.max',
                                                         4).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    presellArgs = kwarguments('PresellInfoItem', '徐州')
    presellDF = spark.read \
                     .format("jdbc") \
                     .options(**presellArgs) \
                     .load() \
                     .fillna("")

    # PresellInfoItem Block --->
    x = presellDF.alias('x')
    # y = houseDF.alias('y')

    # 1. 字段清洗 + 提取
    x = x.withColumn('RecordTime',
                     PresellCoreUDF.record_time_clean(x.RecordTime))
    x = x.withColumn('PresalePermitNumber',
                     PresellCoreUDF.presale_permit_number_clean(
                         x.PresalePermitNumber))
    x = x.withColumn('LandUse', PresellCoreUDF.land_use_clean(x.LandUse))
    # 2. 分组 + 聚合
    # 3. 细节运算
    # 4. 联合入库
    df = x
    columns = df.columns
    for i, c in enumerate(Var.PRESELL_FIELDS):
        if c not in columns:
            df = df.withColumn(c, F.lit(""))
    name_list = set(Var.PRESELL_FIELDS) - set(['ProjectUUID'])
    df = df.select('x.ProjectUUID', *name_list)
    try:
        originArgs = kwarguments('presell_info_xuzhou', '徐州', db='achievement')
        originDF = spark.read \
                     .format("jdbc") \
                     .options(**originArgs) \
                     .load() \
                     .fillna("")
        df = df.unionByName(originDF)
    except Exception as e:
        import traceback
        traceback.print_exc()
    df = df.dropDuplicates(['PresalePermitNumber'])
    df.write.format("jdbc") \
        .options(
            url="jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
            driver="com.mysql.jdbc.Driver",
            dbtable='presell_info_xuzhou',
            user="root",
            password="yunfangdata") \
        .mode("overwrite") \
        .save()
    # <--- PresellInfoItem End Block

    return 0


if __name__ == "__main__":
    main()
