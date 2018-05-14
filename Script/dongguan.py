# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession

from SparkETLCore.CityCore.Dongguan import ProjectCore, BuildingCore,HouseCore
from SparkETLCore.Utils.Var import *
from SparkETLCore.Utils.Var import PROJECT_FIELDS, BUILDING_FIELDS


def kwarguments(tableName, city, groupKey=None, db='spark_test'):
    if groupKey:
        dbtable = '(SELECT * FROM(SELECT * FROM {tableName} WHERE city="{city}" ORDER BY RecordTime DESC) AS col Group BY {groupKey}) {tableName}'.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE city="{city}") {tableName}'.format(
            city=city, tableName=tableName)
    return {
        "url": "jdbc:mysql://10.30.1.70:3307/{}?useUnicode=true&characterEncoding=utf8".format(db),
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": dbtable,
        "user": "root",
        "password": "gh001"
    }


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    row = NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def groupedWork(data, methods, target, fields, tableName):
    df = data
    df = df.rdd.map(lambda r: cleanFields(
        r, methods, target, fields))
    df.toDF().select(fields).write.format("jdbc") \
        .options(
        url="jdbc:mysql://10.30.1.7:3306/mirror?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true",
        driver="com.mysql.jdbc.Driver",
        dbtable=tableName,
        user="root",
        password="yunfangdata") \
        .mode("append") \
        .save()
    return df


appName = 'dongguan'
spark = SparkSession\
    .builder\
    .appName(appName)\
    .config('spark.cores.max', 8)\
    .config('spark.sql.execution.arrow.enabled', "true")\
    .config("spark.sql.codegen", "true")\
    .config("spark.local.dir", "~/.tmp")\
    .getOrCreate()

# Load The Initial DF of Project, Building, Presell, House
# ---
projectArgs = kwarguments('ProjectInfoItem', '东莞', 'ProjectID')
projectDF = spark.read \
                 .format("jdbc") \
                 .options(**projectArgs) \
                 .load() \
                 .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('BuildingInfoItem', '东莞', 'BuildingID')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("BuildingInfoItem")

houseArgs = kwarguments('HouseInfoItem', '东莞', 'HouseID')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("HouseInfoItem")


def projectETL(pjDF=projectDF):
    # Load the DF of Table join with Project
    # Initialize The pre ProjectDF
    # ---
    projectHousingCountDF = spark.sql('''
        select ProjectUUID, count(distinct HouseID) as HousingCount from HouseInfoItem group by ProjectUUID
        ''')
    projectHouseUseTypeDF = spark.sql('''
        select ProjectUUID, concat_ws('@#$', collect_list(distinct HouseUseType)) as HouseUseType from HouseInfoItem group by ProjectUUID
        ''')

    dropColumn = ['HousingCount', 'HouseUseType']
    pjDF = pjDF.drop(*dropColumn).dropDuplicates()
    preProjectDF = pjDF.join(projectHousingCountDF, 'ProjectUUID')\
        .join(projectHouseUseTypeDF, 'ProjectUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, pjDF.columns))
                + [projectHousingCountDF.HousingCount,
                   projectHouseUseTypeDF.HouseUseType])\
        .dropDuplicates()
    print(preProjectDF.count())
    groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                PROJECT_FIELDS, 'project_info_dongguan')

    return 0


def buildingETL(bdDF=buildingDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---

    buildingAddressPresalePermitNumberDF = spark.sql('''
        select ProjectUUID,first(PresalePermitNumber) as PresalePermitNumber,first(ProjectAddress) as Address from (select ProjectUUID, ProjectAddress,PresalePermitNumber from ProjectInfoItem order by RecordTime DESC) as col group by ProjectUUID        ''')
    buildingHousingCountDF = spark.sql('''
        select BuildingUUID, count(distinct HouseID) as HousingCount from HouseInfoItem group by BuildingUUID
        ''')


    dropColumn = ['Address','PresalePermitNumber',
                  'HousingCount']
    bdDF = bdDF.drop(*dropColumn)
    preBuildingDF = bdDF.join(buildingAddressPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .join(buildingAddressPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .join(buildingHousingCountDF, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, bdDF.columns))
                + [buildingAddressPresalePermitNumberDF.PresalePermitNumber,
                   buildingAddressPresalePermitNumberDF.Address,
                   buildingHousingCountDF.HousingCount])\
        .dropDuplicates()
    groupedWork(preBuildingDF, BuildingCore.METHODS, BuildingCore,
                BUILDING_FIELDS, 'building_info_dongguan')

def main():
    return 0

if __name__ == "__main__":
    main()
