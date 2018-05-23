# coding:utf-8
import os
import sys
import datetime
import pyspark
import pandas as pd
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import CaseCore
from SparkETLCore.Utils.Var import ENGINE
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import PROJECT_FIELDS
from SparkETLCore.Utils.Var import BUILDING_FIELDS
from SparkETLCore.Utils.Var import HOUSE_FIELDS
from SparkETLCore.Utils.Var import DEAL_FIELDS
from SparkETLCore.Utils.Var import SUPPLY_FIELDS


def kwarguments(tableName=None, city=None, groupKey=None, query=None,):
    if groupKey:
        dbtable = '''(SELECT * FROM
                    (SELECT * FROM {tableName}
                     WHERE city="{city}"
                     ORDER BY RecordTime DESC) AS col 
                     Group BY {groupKey}) {tableName}'''.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE city="{city}") {tableName}'.format(
            city=city, tableName=tableName)

    return {
        'url': '''jdbc:mysql://10.30.1.70:3307/spark_test?
                    useUnicode=true&characterEncoding=utf-8'''.format(db),
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": query or dbtable,
        "user": "root",
        "password": "yunfangdata"}


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    # row = NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def groupedWork(data, methods, target, fields, tableName):
    df = data
    df = df.rdd.map(lambda r: cleanFields(
        r, methods, target, fields))
    try:
        df = df.toDF().select(fields).write.format("jdbc") \
            .options(
            url='''jdbc:mysql://10.30.1.70:3307/spark_test?
                    useUnicode=true&characterEncoding=utf-8''',
            driver="com.mysql.jdbc.Driver",
            dbtable=tableName,
            user="root",
            password="yunfangdata") \
            .mode("append") \
            .save()
    except ValueError as e:
        import traceback
        traceback.print_exc()
    return df


def projectETL(sc, df):
    bDF = sc.sql(
        '''SELECT ProjectUUID AS BuiProjectUUID,
            count(distinct(BuildingName)) AS BuiHouseBuildingCount,
            concat_ws('@#$', collect_list(distinct PresalePermitNumber)) AS BuiPresalePermitNumber,
            concat_ws('@#$', collect_list(distinct BuildingArea)) AS BuiTotalBuidlingArea,
            concat_ws('@#$', collect_list(ExtraJson)) AS BuiExtraJson
            FROM BuildingInfoItem GROUP BY ProjectUUID''')
    p, b = df.alias('p'), bDF.alias('b')
    p = p.join(b, p.ProjectUUID == b.BuiProjectUUID, 'left').select(p.columns + b.columns)\
        .dropDuplicates().fillna('')

    return groupedWork(p, ProjectCore.METHODS, ProjectCore,
                       PROJECT_FIELDS, 'project_info_hefei')


def buildingETL(sc, df):
    pDF = sc.sql(
        '''SELECT ProjectUUID AS ProProjectUUID,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem GROUP BY ProjectUUID''')
    hDF = sc.sql(
        '''SELECT BuildingUUID AS HouBuildingUUID,
            count(distinct(FloorName)) AS HouFloors,
            FROM HouseInfoItem GROUP BY BuildingUUID''')
    p, b, h = pDF.alias('p'), df.alias('b'), hDF.alias('h')
    b = b.join(p, p.ProProjectUUID == b.ProjectUUID, 'left')\
        .join(h, h.HouBuildingUUID == b.BuildingUUID, 'left')\
        .select(p.columns + b.columns + h.columns).fillna('')

    return groupedWork(b, BuildingCore.METHODS, BuildingCore,
                       BUILDING_FIELDS, 'building_info_hefei')


def houseETL(sc, df):
    pDF = sc.sql(
        '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !=''
            ''')
    bDF = sc.sql(
        '''SELECT ProjectUUID AS BuiProjectUUID,
            first(PresalePermitNumber) AS BuiPresalePermitNumber,
            FROM ProjectInfoItem 
            WHERE DistrictName !=''
            ''')
    p, b, h = pDF.alias('p'), bDF.alias('b'), df.alias('h')
    h = h.join(p, p.ProProjectUUID == h.ProjectUUID, 'left')\
         .join(b, p.ProProjectUUID == b.BuiProjectUUID, 'left')\
        .select(p.columns + b.columns + h.columns).fillna('')

    return groupedWork(h, HouseCore.METHODS, HouseCore,
                       HOUSE_FIELDS, 'house_info_hefei')


def dealCaseETL(sc):
    pDF = sc.sql(
        '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !='' ''')
    bDF = sc.sql(
        '''SELECT ProjectUUID AS BuiProjectUUID,
            first(PresalePermitNumber) AS BuiPresalePermitNumber,
            FROM BuildingInfoItem ''')
    hDF = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('已签约','已备案','已办产权','网签备案单') 
            AND HouseStateLatest in ('可售','抵押可售','摇号销售','现房销售')''')

    p, b, h = pDF.alias('p'), bDF.alias('b'), hDF.alias('h')
    h = h.join(p, p.ProProjectUUID == h.ProjectUUID, 'left')\
         .join(b, p.ProProjectUUID == b.BuiProjectUUID, 'left')\
        .select(p.columns + b.columns + h.columns).fillna('')
    return groupedWork(h, CaseCore.METHODS, CaseCore,
                       DEAL_FIELDS, 'deal_case_hefei')


def supplyCaseETL(sc):
    pDF = sc.sql(
        '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !='' ''')
    bDF = sc.sql(
        '''SELECT ProjectUUID AS BuiProjectUUID,
            first(PresalePermitNumber) AS BuiPresalePermitNumber,
            FROM BuildingInfoItem ''')
    hDF = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售') 
            AND HouseStateLatest=''
            ''')
    p, b, h = pDF.alias('p'), bDF.alias('b'), hDF.alias('h')
    h = h.join(p, p.ProProjectUUID == h.ProjectUUID, 'left')\
        .select(p.columns + b.columns + h.columns).fillna('')
    return groupedWork(h, CaseCore.METHODS, CaseCore,
                       SUPPLY_FIELDS, 'supply_case_hefei')


def quitCaseETL(sc):
    pDF = sc.sql(
        '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !='' ''')
    bDF = sc.sql(
        '''SELECT ProjectUUID AS BuiProjectUUID,
            first(PresalePermitNumber) AS BuiPresalePermitNumber,
            FROM BuildingInfoItem ''')
    hDF = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售')
            AND HouseStateLatest in ('已签约','已备案','已办产权','网签备案单') 
            ''')
    p, b, h = pDF.alias('p'), bDF.alias('b'), hDF.alias('h')
    h = h.join(p, p.ProProjectUUID == h.ProjectUUID, 'left')\
        .select(p.columns + b.columns + h.columns).fillna('')
    return groupedWork(h, CaseCore.METHODS, CaseCore,
                       QUIT_FIELDS, 'quit_case_hefei')


def main(ETL=None):
    sc = SparkSession.builder.appName("Hefei")\
        .config('spark.cores.max', 6)\
        .config('spark.sql.execution.arrow.enabled', 'true')\
        .config('spark.sql.codegen', 'true')\
        .config("spark.local.dir", "~/.tmp").getOrCreate()
    proDF = readPub(sc, table='ProjectInfoItem',
                    city='合肥', groupid='ProjectID')
    buiDF = readPub(sc, table='BuildingInfoItem',
                    city='合肥', groupid='BuildingID')
    houDF = readPub(sc, table='HouseInfoItem', city='合肥', groupid='HouseID')

    if ETL == 'all':
        projectETL(sc, proDF)
        buildingETL(sc, buiDF)
        houseETL(sc, houDF)
        dealCaseETL(sc)
        supplyCaseETL(sc)
    elif ETL == 'projectETL':
        projectETL(sc, proDF)
    elif ETL == 'buildingETL':
        buildingETL(sc, proDF)
    elif ETL == 'houseETL':
        houseETL(sc, proDF)
    elif ETL == 'dealCaseETL':
        dealCaseETL(sc, proDF)
    elif ETL == 'supplyCaseETL':
        supplyCaseETL(sc, proDF)
    elif ETL == 'quitCaseETL':
        quitCaseETL(sc, proDF)
    else:
        pass

if __name__ == '__main__':
    main()
