# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from pyspark.sql import Row, SparkSession
import datetime
from SparkETLCore.CityCore.Dongguan import ProjectCore, BuildingCore, HouseCore, SupplyCaseCore, DealCaseCore, \
    QuitCaseCore
from SparkETLCore.Utils.Var import *


def kwarguments(tableName=None, city=None, groupKey=None, query=None, db='naive'):
    if groupKey:
        dbtable = '(SELECT * FROM(SELECT * FROM {tableName} WHERE city="{city}" ORDER BY RecordTime DESC) AS col Group BY {groupKey}) {tableName}'.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE city="{city}" ORDER BY RecordTime DESC) {tableName}'.format(
            city=city, tableName=tableName)
    return {
        "url": "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8".format(db),
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": query or dbtable,
        "user": "root",
        "password": "yunfangdata"
    }


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    row = NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def groupedWork(data, methods, target, fields, tableName, distinctKey=None):
    res = None
    maxRecordTime = "1970-01-01 00:00:00"
    df = data
    argsDictRead = {'url': "jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
                    'driver': "com.mysql.jdbc.Driver",
                    'dbtable': "(select max(RecordTime) as RecordTime from {tableName}) {tableName}".format(tableName=tableName),
                    'user': "root",
                    'password': "yunfangdata"}
    argsDictWrite = {'url': "jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
                     'driver': "com.mysql.jdbc.Driver",
                     'dbtable': tableName,
                     'user': "root",
                     'password': "yunfangdata"}
    try:
        maxRecordTime = spark.read\
            .format("jdbc")\
            .options(**argsDictRead)\
            .load()\
            .fillna("")\
            .first().RecordTime
    except Exception:
        import traceback
        traceback.print_exc()
    df = df.filter("RecordTime>='%s'" % str(maxRecordTime))
    df = df.rdd.repartition(1000).map(lambda r: cleanFields(
        r, methods, target, fields)).toDF().select(fields)
    if distinctKey:
        df = df.dropDuplicates(distinctKey)
    res = df.write.format("jdbc") \
        .options(**argsDictWrite) \
        .mode("append") \
        .save()
    return res


city = "Dongguan"
if len(sys.argv) < 2:
    appName = 'test'
else:
    appName = '_'.join([city, sys.argv[1]])
spark = SparkSession \
    .builder \
    .appName(appName) \
    .config('spark.cores.max', 6) \
    .config('spark.sql.execution.arrow.enabled', "true") \
    .config("spark.sql.codegen", "true") \
    .getOrCreate()

projectArgs = kwarguments(tableName='projectinfoitem',
                          city='东莞', groupKey='ProjectID')
projectDF = spark.read \
    .format("jdbc") \
    .options(**projectArgs) \
    .load() \
    .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments(
    tableName='buildinginfoitem', city='东莞', groupKey='BuildingID')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("BuildingInfoItem")

houseArgs = kwarguments(tableName='houseinfoitem',
                        city='东莞', groupKey='HouseID')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("HouseInfoItem")

supplyArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="东莞" 
    AND HouseState in ('可售','待售') 
    AND HouseStateLatest in ('可售', '待售', '')) SupplyInfoItem
    ''')
supplyDF = spark.read \
    .format("jdbc") \
    .options(**supplyArgs) \
    .load() \
    .fillna("")
supplyDF.createOrReplaceTempView("SupplyInfoItem")

dealArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="东莞" 
    AND HouseState in ('不可售','已售') 
    AND HouseStateLatest in ('可售', '待售', '')) DealInfoItem
    ''')
dealDF = spark.read \
    .format("jdbc") \
    .options(**dealArgs) \
    .load() \
    .fillna("")
dealDF.createOrReplaceTempView("DealInfoItem")

quitArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="东莞" 
    AND HouseState in ('可售','待售') 
    AND HouseStateLatest in ('不可售', '已售')) QuitInfoItem
    ''')
quitDF = spark.read \
    .format("jdbc") \
    .options(**quitArgs) \
    .load() \
    .fillna("")
quitDF.createOrReplaceTempView("QuitInfoItem")


def projectETL(pjDF=projectDF):
    projectHouseDF = spark.sql('''
        select ProjectUUID, 
        concat_ws('@#$', collect_list(distinct HouseUseType)) as HouseUseType 
        from HouseInfoItem  group by ProjectUUID
        ''')

    dropColumn = ['HouseUseType']
    pjDF = pjDF.drop(*dropColumn).dropDuplicates()
    preProjectDF = pjDF.join(projectHouseDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, pjDF.columns))
                + [projectHouseDF.HouseUseType]) \
        .dropDuplicates()
    print(preProjectDF.count())
    groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                PROJECT_FIELDS, 'project_info_dongguan', ['ProjectID'])


def buildingETL(bdDF=buildingDF):
    buildingAddressPresalePermitNumberDF = spark.sql('''
       select ProjectUUID, ProjectAddress as Address from ProjectInfoItem
        ''')

    dropColumn = ['Address']
    bdDF = bdDF.drop(*dropColumn)
    preBuildingDF = bdDF.join(buildingAddressPresalePermitNumberDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, bdDF.columns))
                + [buildingAddressPresalePermitNumberDF.Address]) \
        .dropDuplicates()
    groupedWork(preBuildingDF, BuildingCore.METHODS, BuildingCore,
                BUILDING_FIELDS, 'building_info_dongguan', ['BuildingID'])


def houseETL(hsDF=houseDF):
    districtNameDF = spark.sql('''
        select ProjectUUID, ProjectAddress as Address,DistrictName from ProjectInfoItem
           ''')

    dropColumn = ['Address', 'DistrictName']
    hsDF = hsDF.drop(*dropColumn)
    preHouseDF = hsDF.join(districtNameDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, hsDF.columns))
                + [districtNameDF.DistrictName,
                   districtNameDF.Address]) \
        .dropDuplicates()
    groupedWork(preHouseDF, HouseCore.METHODS, HouseCore,
                HOUSE_FIELDS, 'house_info_dongguan', ['HouseID'])


def supplyETL(spDF=supplyDF):
    projectSupplyDF = spark.sql('''
        select ProjectUUID, RegionName,ProjectAddress as Address,DistrictName, PresalePermitNumber from ProjectInfoItem         
        ''')
    BuildingHouseDF = spark.sql('''
        select BuildingUUID, Floors from BuildingInfoItem      
            ''')
    dropColumn = ['Address', 'DistrictName',
                  'PresalePermitNumber', 'RegionName', 'Floors']
    spDF = spDF.drop(*dropColumn)
    preHouseDF = spDF.join(projectSupplyDF, 'ProjectUUID', 'left') \
        .join(BuildingHouseDF, 'BuildingUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, spDF.columns))
                + [projectSupplyDF.DistrictName,
                   projectSupplyDF.Address,
                   projectSupplyDF.RegionName,
                   BuildingHouseDF.Floors]) \
        .dropDuplicates()
    groupedWork(preHouseDF, SupplyCaseCore.METHODS, SupplyCaseCore,
                SUPPLY_FIELDS, 'supply_case_dongguan', ['RecordTime', 'HouseID'])


def dealETL(dealDF=dealDF):
    projectSupplyDF = spark.sql('''
        select ProjectUUID, RegionName,ProjectAddress as Address,DistrictName, PresalePermitNumber from ProjectInfoItem         
        ''')
    BuildingHouseDF = spark.sql('''
        select BuildingUUID, Floors from BuildingInfoItem      
            ''')
    dropColumn = ['Address', 'DistrictName',
                  'PresalePermitNumber', 'RegionName', 'Floors']
    dealDF = dealDF.drop(*dropColumn)
    preHouseDF = dealDF.join(projectSupplyDF, 'ProjectUUID', 'left') \
        .join(BuildingHouseDF, 'BuildingUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, dealDF.columns))
                + [projectSupplyDF.DistrictName,
                   projectSupplyDF.Address,
                   projectSupplyDF.RegionName,
                   BuildingHouseDF.Floors]) \
        .dropDuplicates()
    groupedWork(preHouseDF, DealCaseCore.METHODS, DealCaseCore,
                DEAL_FIELDS, 'deal_case_dongguan', ['RecordTime', 'HouseID'])


def quitETL(quitDF=quitDF):
    projectSupplyDF = spark.sql('''
        select ProjectUUID, RegionName,ProjectAddress as Address,DistrictName, PresalePermitNumber from ProjectInfoItem         
        ''')
    BuildingHouseDF = spark.sql('''
        select BuildingUUID, Floors from BuildingInfoItem      
            ''')
    dropColumn = ['Address', 'DistrictName',
                  'PresalePermitNumber', 'RegionName', 'Floors']
    quitDF = quitDF.drop(*dropColumn)
    preHouseDF = quitDF.join(projectSupplyDF, 'ProjectUUID', 'left') \
        .join(BuildingHouseDF, 'BuildingUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, quitDF.columns))
                + [projectSupplyDF.DistrictName,
                   projectSupplyDF.Address,
                   projectSupplyDF.RegionName,
                   BuildingHouseDF.Floors]) \
        .dropDuplicates()
    groupedWork(preHouseDF, QuitCaseCore.METHODS, QuitCaseCore,
                QUIT_FIELDS, 'quit_case_dongguan', ['RecordTime', 'HouseID'])


def main():
    methodsDict = {
        'projectETL': projectETL,
        'buildingETL': buildingETL,
        'houseETL': houseETL,
        'dealETL': dealETL,
        'supplyETL': supplyETL,
        'quitETL': quitETL,
    }
    if len(sys.argv) < 2:
        return 0
    else:
        methodInstance = methodsDict.get(sys.argv[1])
        if methodInstance:
            methodInstance()
    return 0


if __name__ == "__main__":
    main()
