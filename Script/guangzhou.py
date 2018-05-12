# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import Row, SparkSession

from SparkETLCore.CityCore.Guangzhou import ProjectCore, BuildingCore
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
    df = df.toDF().select(fields).write.format("jdbc") \
        .options(
        url="jdbc:mysql://10.30.1.7:3306/mirror?useUnicode=true&characterEncoding=utf8",
        driver="com.mysql.jdbc.Driver",
        dbtable=tableName,
        user="root",
        password="yunfangdata") \
        .mode("append") \
        .save()
    return df


appName = 'guangzhou'
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
projectArgs = kwarguments('ProjectInfoItem', '广州', 'ProjectID')
projectDF = spark.read \
                 .format("jdbc") \
                 .options(**projectArgs) \
                 .load() \
                 .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('BuildingInfoItem', '广州', 'BuildingID')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("BuildingInfoItem")

houseArgs = kwarguments('HouseInfoItem', '广州', 'HouseID')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("HouseInfoItem")

presellArgs = kwarguments('PresellInfoItem', '广州', 'PresalePermitNumber')
presellDF = spark.read \
                 .format("jdbc") \
                 .options(**presellArgs) \
                 .load() \
                 .fillna("")
presellDF.createOrReplaceTempView("PresellInfoItem")


def main():
    return 0


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
    projectLssueDateDF = spark.sql('''
        select ProjectUUID, concat_ws('@#$', collect_list(distinct LssueDate)) as LssueDate from PresellInfoItem group by ProjectUUID
        ''')
    projectPresalePermitNumberDF = spark.sql('''
        select ProjectUUID, concat_ws('@#$', collect_list(distinct PresalePermitNumber)) as PresalePermitNumber from PresellInfoItem group by ProjectUUID
        ''')
    projectHouseBuildingCountDF = spark.sql('''
        select ProjectUUID, count(distinct BuildingName) as HouseBuildingCount from BuildingInfoItem group by ProjectUUID
        ''')
    dropColumn = ['HousingCount', 'HouseUseType', 'LssueDate',
                  'PresalePermitNumber', 'HouseBuildingCount']
    pjDF = pjDF.drop(*dropColumn).dropDuplicates()
    # print(pjDF.count())
    preProjectDF = pjDF.join(projectHousingCountDF, 'ProjectUUID')\
        .join(projectHouseBuildingCountDF, 'ProjectUUID', 'left')\
        .join(projectHouseUseTypeDF, 'ProjectUUID', 'left')\
        .join(projectLssueDateDF, 'ProjectUUID', 'left')\
        .join(projectPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, pjDF.columns))
                + [projectHousingCountDF.HousingCount,
                   projectHouseUseTypeDF.HouseUseType,
                   projectLssueDateDF.LssueDate,
                   projectPresalePermitNumberDF.PresalePermitNumber,
                   projectHouseBuildingCountDF.HouseBuildingCount])\
        .dropDuplicates()
    # print(preProjectDF.count())
    return groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                       PROJECT_FIELDS, 'project_info_guangzhou')


def buildingETL(bdDF=buildingDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    buildingPresalePermitNumberDF = spark.sql('''
        select ProjectUUID, concat_ws('@#$', collect_list(distinct PresalePermitNumber)) as PresalePermitNumber from PresellInfoItem group by ProjectUUID
        ''')
    buildingAddressDF = spark.sql('''
        select ProjectUUID, ProjectAddress as Address from (select ProjectUUID, ProjectAddress from ProjectInfoItem order by RecordTime DESC) as col group by ProjectUUID
        ''')
    buildingHousingCountDF = spark.sql('''
        select BuildingUUID, count(distinct HouseID) as HousingCount from HouseInfoItem group by BuildingUUID
        ''')
    buildingFloorsDF = spark.sql('''
        select BuildingUUID, count(distinct ActualFloor) as Floors from HouseInfoItem group by BuildingUUID
        ''')
    buildingBuildingStructureDF = spark.sql('''
        select BuildingUUID, concat_ws('@#$', collect_list(distinct BuildingStructure)) as BuildingStructure from HouseInfoItem group by BuildingUUID
        ''')
    dropColumn = ['PresalePermitNumber', 'Address',
                  'HousingCount', 'Floors', 'BuildingStructure']
    bdDF = bdDF.drop(*dropColumn)
    preBuildingDF = bd.join(buildingPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .join(buildingAddressDF, 'ProjectUUID', 'left')\
        .join(buildingHousingCountDF, 'BuildingUUID', 'left')\
        .join(buildingFloorsDF, 'BuildingUUID', 'left')\
        .join(buildingBuildingStructureDF, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, bdDF.columns))
                + [buildingPresalePermitNumberDF.PresalePermitNumber,
                   buildingAddressDF.Address,
                   buildingHousingCountDF.HousingCount,
                   buildingFloorsDF.Floors,
                   buildingBuildingStructureDF.BuildingStructure])\
        .dropDuplicates()
    return groupedWork(preBuildingDF, BuildingCore.METHODS, BuildingCore,
                       BUILDING_FIELDS, 'building_info_guangzhou')


if __name__ == "__main__":
    main()
