# coding:utf-8
import os
import sys
import datetime
import pyspark
import pandas as pd
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import PresellCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import CaseCore
from SparkETLCore.Utils.Var import ENGINE
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import PROJECT_FIELDS
from SparkETLCore.Utils.Var import BUILDING_FIELDS
from SparkETLCore.Utils.Var import PRESELL_FIELDS
from SparkETLCore.Utils.Var import HOUSE_FIELDS
from SparkETLCore.Utils.Var import DEAL_FIELDS
from SparkETLCore.Utils.Var import SUPPLY_FIELDS 

# input sc,mysql's tablename,groupby fileds;making a tempview and return dataframe 
def readPub (sc,table,city,groupid=None):
    if groupid:
        options={'url':'''jdbc:mysql://10.30.1.70:3307/spark_test?
                                 useUnicode=true&characterEncoding=utf-8''',
                        'driver':"com.mysql.jdbc.Driver",
                        "user":"root","password":"gh001",
                        "dbtable":'''(SELECT * FROM 
                                     (SELECT * FROM {table} 
                                     WHERE City='{city}' AND {groupid} !=''
                                     ORDER BY RecordTime DESC ) AS col 
                                     Group BY col.{groupid}) {table}'''
                                     .format(table=table,city=city,groupid=groupid)}
    else:
        options={'url':'''jdbc:mysql://10.30.1.70:3307/spark_test?
                                 useUnicode=true&characterEncoding=utf-8''',
                        'driver':"com.mysql.jdbc.Driver",
                        "user":"root","password":"gh001",
                        "dbtable":'''(SELECT * FROM {table} 
                                     WHERE City='{city}'
                                     ORDER BY RecordTime DESC ) {table}'''
                                     .format(table=table,city=city)}
    df = sc.read.format("jdbc").options(**options).load().fillna('')
    df.createOrReplaceTempView(table)
    return df

# input df,and jdbc's options;writing into mysql 
def writePub (df,option):
    options = {'url':'''jdbc:mysql://10.30.1.70:3307/spark_caches?
                            useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true''',
                    'driver':"com.mysql.jdbc.Driver",
                    "user":"root",
                    "password":"gh001"}
    options.update(option)
    df.write.format("jdbc").mode('append').options(**options).save()
    return 'finish'

# using SparkETLCore for each row
def cleanFields (data,cls):
    data = data.asDict()
    for index,method in enumerate(cls.METHODS):
        func = getattr(cls,method)
        data = func(data)
    data = Row(**data)
    return data

def projectETL(sc,df):
    bDF = sc.sql(
            '''SELECT ProjectUUID AS BuiProjectUUID,
            count(distinct(BuildingName)) AS BuiHouseBuildingCount,
            concat_ws('@#$', collect_list(distinct PresalePermitNumber)) AS BuiPresalePermitNumber,
            concat_ws('@#$', collect_list(distinct BuildingArea)) AS BuiTotalBuidlingArea,
            concat_ws('@#$', collect_list(ExtraJson)) AS BuiExtraJson
            FROM BuildingInfoItem GROUP BY ProjectUUID''')
    p,b = df.alias('p'),bDF.alias('b')
    p = p.join(b,p.ProjectUUID==b.BuiProjectUUID,'left').select(p.columns+b.columns)\
                .dropDuplicates().fillna('')

    cleandf = p.rdd.map(lambda r :cleanFields(r,ProjectCore)).toDF().select(PROJECT_FIELDS)
    result = writePub(cleandf,{'dbtable':"project_info_hefei"})

def buildingETL(sc,df):
    pDF = sc.sql(
            '''SELECT ProjectUUID AS ProProjectUUID,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem GROUP BY ProjectUUID''')
    hDF = sc.sql(
            '''SELECT BuildingUUID AS HouBuildingUUID,
            count(distinct(FloorName)) AS HouFloors,
            FROM HouseInfoItem GROUP BY BuildingUUID''')
    p,b,h = pDF.alias('p'),df.alias('b'),hDF.alias('h')
    b = b.join(p,p.ProProjectUUID==b.ProjectUUID,'left')\
          .join(h,h.HouBuildingUUID==b.BuildingUUID,'left')\
          .select(p.columns+b.columns+h.columns).fillna('')

    cleandf = b.rdd.map(lambda r :cleanFields(r,BuildingCore)).toDF().select(BUILDING_FIELDS)
    result = writePub(cleandf,{'dbtable':"building_info_hefei"})

def houseETL(sc,df):
    pDF = sc.sql(
            '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !=''
            ''')
    p,h = pDF.alias('p'),df.alias('h')
    h = h.join(p,p.ProProjectUUID==h.ProjectUUID,'left')\
          .select(p.columns+h.columns).fillna('')

    cleandf = h.rdd.map(lambda r :cleanFields(r,HouseCore)).toDF().select(HOUSE_FIELDS)
    result = writePub(cleandf,{'dbtable':"house_info_hefei"})

def dealCaseETL(sc):
    pDF = sc.sql(
            '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem 
            WHERE DistrictName !='' 
            GROUP BY ProjectUUID''')
    hDF = sc.sql(
            '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('已签约','已备案','已办产权','网签备案单') 
            AND HouseStateLatest in ('可售','抵押可售','摇号销售','现房销售')
            ''')
    p,h = pDF.alias('p'),hDF.alias('h')
    hh = h.join(p,p.ProProjectUUID==h.ProjectUUID,'left')\
          .select(p.columns+h.columns).fillna('')
          
    cleandf = hh.rdd.map(lambda r :cleanFields(r,CaseCore)).toDF().select(DEAL_FIELDS)
    result = writePub(cleandf,{'dbtable':"deal_case_hefei"})

def supplyCaseETL(sc):
    pDF = sc.sql(
            '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem WHERE DistrictName !='' 
            GROUP BY ProjectID''')
    hDF = sc.sql(
            '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售') 
            AND HouseStateLatest=''
            ''')
    p,h = pDF.alias('p'),hDF.alias('h')
    hh = h.join(p,p.ProProjectUUID==h.ProjectUUID,'left')\
          .select(p.columns+h.columns).fillna('')
    cleandf = hh.rdd.map(lambda r :cleanFields(r,CaseCore)).toDF().select(SUPPLY_FIELDS)
    result = writePub(cleandf,{'dbtable':"supply_case_hefei"})

def quitCaseETL(sc):
    pDF = sc.sql(
            '''SELECT ProjectUUID AS ProProjectUUID,
            first(DistrictName) AS ProDistrictName,
            first(ProjectAddress) AS ProProjectAddress
            FROM ProjectInfoItem WHERE DistrictName !='' 
            GROUP BY ProjectUUID''')
    hDF = sc.sql(
            '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售')
            AND HouseStateLatest in ('已签约','已备案','已办产权','网签备案单') 
            ''')
    p,h = pDF.alias('p'),hDF.alias('qh')
    qh = qh.join(p,p.ProProjectUUID==h.ProjectUUID,'left')\
          .select(p.columns+h.columns).fillna('')
    cleandf = qh.rdd.map(lambda r :cleanFields(r,CaseCore)).toDF().select(QUIT_FIELDS)
    result = writePub(cleandf,{'dbtable':"deal_case_hefei"})

def main(ETL=None):
    sc =SparkSession.builder.appName("Hefei")\
            .config('spark.cores.max',6)\
            .config('spark.sql.execution.arrow.enabled','true')\
            .config('spark.sql.codegen','true')\
            .config("spark.local.dir", "~/.tmp").getOrCreate()
    proDF = readPub(sc,table='ProjectInfoItem',city='合肥',groupid='ProjectID')
    buiDF = readPub(sc,table='BuildingInfoItem',city='合肥',groupid='BuildingID')
    houDF = readPub(sc,table='HouseInfoItem',city='合肥',groupid='HouseID')
    
    if ETL =='all':
        projectETL(sc,proDF)
        buildingETL(sc,buiDF)
        houseETL(sc,houDF)
        dealCaseETL(sc)
        supplyCaseETL(sc)
    elif ETL=='projectETL':
        projectETL(sc,proDF)
    elif ETL=='buildingETL':
        buildingETL(sc,proDF)
    elif ETL=='houseETL':
        houseETL(sc,proDF)
    elif ETL=='dealCaseETL':
        dealCaseETL(sc,proDF)
    elif ETL=='supplyCaseETL':
        supplyCaseETL(sc,proDF)
    elif ETL=='quitCaseETL':
        quitCaseETL(sc,proDF)
    else:
        pass

if __name__ == '__main__':
    main()
    