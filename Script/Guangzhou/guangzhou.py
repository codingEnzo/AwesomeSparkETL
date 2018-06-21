# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from SparkETLCore.Utils.Var import *
from SparkETLCore.CityCore.Guangzhou import ProjectCore, BuildingCore, PresellCore, HouseCore, DealCaseCore, SupplyCaseCore, QuitCaseCore


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


city = "Guangzhou"
if len(sys.argv) < 2:
    appName = 'test'
else:
    appName = '_'.join([city, sys.argv[1]])

spark = SparkSession \
    .builder \
    .appName(appName) \
    .config('spark.cores.max', 6) \
    .getOrCreate()

# Load The Initial DF of Project, Building, Presell, House
# ---
projectArgs = kwarguments('projectinfoitem', '广州', 'ProjectID')
projectDF = spark.read \
    .format("jdbc") \
    .options(**projectArgs) \
    .load() \
    .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('buildinginfoitem', '广州', 'BuildingID')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("BuildingInfoItem")

houseArgs = kwarguments('houseinfoitem', '广州', 'HouseID')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("HouseInfoItem")

dealArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="广州"
    AND find_in_set("已签约",HouseLabel) AND ! find_in_set("已签约",HouseLabelLatest)
    AND HouseUseType!="详见附注" AND HouseStateLatest !="") DealInfoItem
    ''')
dealDF = spark.read \
    .format("jdbc") \
    .options(**dealArgs) \
    .load() \
    .fillna("")
dealDF.createOrReplaceTempView("DealInfoItem")

supplyArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="广州"
    AND HouseState in ("预售可售","确权可售")
    AND HouseStateLatest in ("预售可售","确权可售")) SupplyInfoItem
    ''')
supplyDF = spark.read \
    .format("jdbc") \
    .options(**supplyArgs) \
    .load() \
    .fillna("")
supplyDF.createOrReplaceTempView("SupplyInfoItem")

quitArgs = kwarguments(query='''
    (SELECT * FROM houseinfoitem
    WHERE City="广州"
    AND HouseLabel ="" AND find_in_set("已签约",HouseLabelLatest)
    AND HouseState in ("预售可售","确权可售") AND find_in_set("已签约",HouseState)
    AND find_in_set("不可",HouseStateLatest)) QuitInfoItem
    ''')
quitDF = spark.read \
    .format("jdbc") \
    .options(**quitArgs) \
    .load() \
    .fillna("")
quitDF.createOrReplaceTempView("QuitInfoItem")

presellArgs = kwarguments('PresellInfoItem', '广州', 'PresalePermitNumber')
presellDF = spark.read \
    .format("jdbc") \
    .options(**presellArgs) \
    .load() \
    .fillna("")
presellDF.createOrReplaceTempView("PresellInfoItem")


def projectETL(pjDF=projectDF):
    # Load the DF of Table join with Project
    # Initialize The pre ProjectDF
    # ---
    projectHousingInfoDF = spark.sql('''
        select ProjectUUID, count(distinct HouseID) as HousingCount, concat_ws('@#$', collect_list(distinct HouseUseType)) as HouseUseType from HouseInfoItem group by ProjectUUID
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
    preProjectDF = pjDF.join(projectHousingInfoDF, 'ProjectUUID')\
        .join(projectHouseBuildingCountDF, 'ProjectUUID', 'left')\
        .join(projectLssueDateDF, 'ProjectUUID', 'left')\
        .join(projectPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, pjDF.columns))
                + [projectHousingInfoDF.HousingCount,
                   projectHousingInfoDF.HouseUseType,
                   projectLssueDateDF.LssueDate,
                   projectPresalePermitNumberDF.PresalePermitNumber,
                   projectHouseBuildingCountDF.HouseBuildingCount])\
        .dropDuplicates()
    # print(preProjectDF.count())
    return groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                       PROJECT_FIELDS, 'project_info_guangzhou', ['ProjectID'])


def buildingETL(bdDF=buildingDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    buildingPresalePermitNumberDF = spark.sql('''
        select ProjectUUID, concat_ws('@#$', collect_list(distinct PresalePermitNumber)) as PresalePermitNumber from PresellInfoItem group by ProjectUUID
        ''')
    buildingAddressDF = spark.sql('''
        select ProjectUUID, first(ProjectAddress) as Address from (select ProjectUUID, ProjectAddress from ProjectInfoItem) as col group by col.ProjectUUID
        ''')
    buildingInfoDF = spark.sql('''
        select
            BuildingUUID, concat_ws('@#$', collect_list(distinct BuildingStructure)) as BuildingStructure,
            count(distinct HouseID) as HousingCount, count(distinct ActualFloor) as Floors
            from HouseInfoItem group by BuildingUUID
        ''')
    dropColumn = ['PresalePermitNumber', 'Address',
                  'HousingCount', 'Floors', 'BuildingStructure']
    bdDF = bdDF.drop(*dropColumn)
    preBuildingDF = bdDF.join(buildingPresalePermitNumberDF, 'ProjectUUID', 'left')\
        .join(buildingAddressDF, 'ProjectUUID', 'left')\
        .join(buildingInfoDF, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, bdDF.columns))
                + [buildingPresalePermitNumberDF.PresalePermitNumber,
                   buildingAddressDF.Address,
                   buildingInfoDF.HousingCount,
                   buildingInfoDF.Floors,
                   buildingInfoDF.BuildingStructure])\
        .dropDuplicates()
    return groupedWork(preBuildingDF, BuildingCore.METHODS, BuildingCore,
                       BUILDING_FIELDS, 'building_info_guangzhou', ['BuildingID'])


def presellETL(psDF=presellDF):
    # Load the DF of Table join with Presell
    # Initialize The pre PresellDF
    # ---
    dropColumn = []
    psDF = psDF.drop(*dropColumn)
    prePresellDF = psDF.dropDuplicates()
    return groupedWork(prePresellDF, PresellCore.METHODS, PresellCore,
                       PRESELL_FIELDS, 'presell_info_guangzhou', ['PresalePermitNumber'])


def houseETL(hsDF=houseDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    projectInfoDF = spark.sql('''
        select ProjectUUID, ProjectAddress as Address, DistrictName from ProjectInfoItem
        ''')
    dropColumn = ['Address', 'DistrictName']
    hsDF = hsDF.drop(*dropColumn)
    preHouseDF = hsDF.join(projectInfoDF, 'ProjectUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, hsDF.columns))
                + [projectInfoDF.DistrictName,
                   projectInfoDF.Address])\
        .dropDuplicates()
    return groupedWork(preHouseDF, HouseCore.METHODS, HouseCore,
                       HOUSE_FIELDS, 'house_info_guangzhou', ['HouseID'])


def dealETL(dlDF=dealDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    # dlDF = dlDF.filter("RecordTime>='%s'" % str(
    #     datetime.datetime.now() - datetime.timedelta(days=7)))
    projectRawDF = spark.read \
        .format("jdbc") \
        .options(**kwarguments('ProjectInfoItem', '广州')) \
        .load() \
        .select('ProjectUUID', 'ExtraJson', F.row_number()
                .over(Window.partitionBy('ProjectUUID')
                      .orderBy(F.desc('RecordTime')))
                .alias('Rn')) \
        .fillna("")
    projectRawDF.createOrReplaceTempView('PriceRaw')
    dealPriceDF = spark.sql("""
            select ProjectUUID, concat_ws('@#$', collect_list(distinct ExtraJson)) as Price from
            ((select ProjectUUID, ExtraJson from
            PriceRaw where Rn <= 2) as col2) group by ProjectUUID
        """)
    projectInfoDF = spark.sql('''
        select ProjectUUID, DistrictName, RegionName, ProjectAddress as Address, PresalePermitNumber from ProjectInfoItem
        ''')
    dropColumn = ['Address', 'DistrictName',
                  'RegionName', 'PresalePermitNumber', 'Price', 'TotalPrice']
    dlDF = dlDF.drop(*dropColumn)
    preDealDF = dlDF.join(projectInfoDF, 'ProjectUUID', 'left')\
        .join(dealPriceDF, 'ProjectUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, dlDF.columns))
                + [projectInfoDF.DistrictName,
                   projectInfoDF.Address,
                   projectInfoDF.RegionName,
                   projectInfoDF.PresalePermitNumber,
                   dealPriceDF.Price])\
        .dropDuplicates()\
        .fillna('')
    return groupedWork(preDealDF, DealCaseCore.METHODS, DealCaseCore,
                       DEAL_FIELDS, 'deal_case_guangzhou', ['RecordTime', 'HouseID'])


def supplyETL(spDF=supplyDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    # spDF = spDF.filter("RecordTime>='%s'" % str(
    #     datetime.datetime.now() - datetime.timedelta(days=7)))
    projectInfoDF = spark.sql('''
        select ProjectUUID, DistrictName, RegionName, ProjectAddress as Address, PresalePermitNumber from ProjectInfoItem
        ''')
    dropColumn = ['Address', 'DistrictName',
                  'RegionName', 'PresalePermitNumber']
    spDF = spDF.drop(*dropColumn)
    preSupplyDF = spDF.join(projectInfoDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, spDF.columns))
                + [projectInfoDF.DistrictName,
                   projectInfoDF.Address,
                   projectInfoDF.RegionName,
                   projectInfoDF.PresalePermitNumber])\
        .dropDuplicates()\
        .fillna('')
    return groupedWork(preSupplyDF, SupplyCaseCore.METHODS, SupplyCaseCore,
                       SUPPLY_FIELDS, 'supply_case_guangzhou', ['RecordTime', 'HouseID'])


def quitETL(qtDF=quitDF):
    # Load the DF of Table join with Building
    # Initialize The pre BuildingDF
    # ---
    # qtDF = qtDF.filter("RecordTime>='%s'" % str(
    #     datetime.datetime.now() - datetime.timedelta(days=7)))
    projectInfoDF = spark.sql('''
        select ProjectUUID, DistrictName, RegionName, ProjectAddress as Address, PresalePermitNumber from ProjectInfoItem
        ''')
    dropColumn = ['Address', 'DistrictName',
                  'RegionName', 'PresalePermitNumber']
    qtDF = qtDF.drop(*dropColumn)
    preQuitDF = qtDF.join(projectInfoDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, qtDF.columns))
                + [projectInfoDF.DistrictName,
                   projectInfoDF.Address,
                   projectInfoDF.RegionName,
                   projectInfoDF.PresalePermitNumber])\
        .dropDuplicates()\
        .fillna('')
    return groupedWork(preQuitDF, QuitCaseCore.METHODS, QuitCaseCore,
                       QUIT_FIELDS, 'quit_case_guangzhou', ['RecordTime', 'HouseID'])


def main():
    methodsDict = {'projectETL': projectETL,
                   'buildingETL': buildingETL,
                   'presellETL': presellETL,
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
