# coding:utf-8
import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from sqlalchemy import *
from pyspark.sql.types import *
sys.path.append(os.path.dirname(os.getcwd()))
from SparkETLCore.CityCore.Wulumuqi import ProjectCore
from SparkETLCore.CityCore.Wulumuqi import BuildingCore
from SparkETLCore.CityCore.Wulumuqi import PresellCore
from SparkETLCore.CityCore.Wulumuqi import HouseCore
from SparkETLCore.CityCore.Wulumuqi import CaseCore
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import ENGINE
import datetime


def readPub (sc,options):
    options.update({'url':'''jdbc:mysql://10.30.1.70:3307/spark_test?
                            useUnicode=true&characterEncoding=utf-8''',
                    'driver':"com.mysql.jdbc.Driver",
                    "user":"root",
                    "password":"gh001"})
    df = sc.read.format("jdbc").options(**options).load()
    return df.fillna('')

def savePub (df,options):
    options.update({'url':'''jdbc:mysql://10.30.1.70:3307/spark_caches?
                            useUnicode=true&characterEncoding=utf-8''',
                    'driver':"com.mysql.jdbc.Driver",
                    "user":"root",
                    "password":"gh001"})
    df.write.format("jdbc").mode('append').options(**options).save()
    return 'finish'

def coreCleanPub (data,cls):
    for index,method in enumerate(cls.METHODS):
        func = getattr(cls,method)
        data = func(data)
    return data

def projectClean(searchtime='1970-01-01'):
    print ('project clean is going run')
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = {'dbtable':'''(SELECT * FROM 
                (SELECT * FROM ProjectInfoItem 
                WHERE City='乌鲁木齐' AND RecordTime>'{0}'AND ProjectID !=''
                ORDER BY RecordTime DESC ) AS col 
                Group BY col.ProjectID LIMIT 1) tmp'''.format(searchtime)}
    optionSave = {'dbtable':"project_info_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,ProjectCore)).toDF()
    result = savePub(cleandf,optionSave)
    print (result)

def buildingClean(searchtime='1970-01-01'):
    print ('building script is going run')
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = { 'dbtable':'''(SELECT * FROM 
                    (SELECT * FROM BuildingInfoItem 
                    WHERE City='乌鲁木齐' AND RecordTime>'{0}'AND BuildingID !=''
                    ORDER BY RecordTime DESC ) AS col 
                    Group BY col.BuildingID LIMIT 10)tmp'''.format(searchtime)}
    optionSave = {'dbtable':"building_info_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,BuildingCore)).toDF()
    result = savePub(cleandf,optionSave)
    print ('building'+result)

def permitClean(searchtime='1970-01-01'):
    #没有预售证表格不分离数据
    pass

def houseClean(searchtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('house script is going run')
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = {
                  # 'dbtable':'''(SELECT * FROM 
                  #   (SELECT * FROM HouseInfoItem 
                  #   WHERE City='乌鲁木齐' AND RecordTime>'{0}'AND HouseID !=''
                  #   ORDER BY RecordTime DESC ) AS col 
                  #   Group BY col.HouseID LIMIT 4)tmp'''.format(searchtime)
                  'dbtable':'''(SELECT * FROM HouseInfoItem 
                    WHERE HouseUUID ='0001de15-6b90-3d63-b3ab-88c25e40e678')tmp'''}
    optionSave = {'dbtable':"house_info_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,HouseCore)).toDF()
    result = savePub(cleandf,optionSave)
    print ('house'+result)

def dealCaseClean(searchtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('dealcase script is going run')
    dropLT =['HouseStateLatest','ExtraJson']
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = {'dbtable':'''(SELECT * FROM 
                        (SELECT SourceUrl as SourceLink,
                        RecordTime as CaseTime,
                        FloorName as ActualFloor,
                        HouseState as SellState,
                        DistrictName,ProjectName,BuildingName,
                        IsMortgage,IsAttachment,IsPrivateUse,IsMoveBack,
                        MeasuredInsideOfBuildingArea,MeasuredSharedPublicArea,
                        IsSharedPublicMatching,BuildingStructure,SellSchedule,
                        UnitShape,UnitStructure,Balconys,UnenclosedBalconys,
                        HouseName,HouseNumber,TotalPrice,Price,PriceType,Address,
                        FloorName,HouseUseType,Dwelling,Remarks,RecordTime,
                        ProjectUUID,BuildingUUID,HouseUUID,UnitUUID,ExtraJson
                        BuildingID,HouseID,ForecastBuildingArea,ForecastInsideOfBuildingArea,
                        RealEstateProjectID,HouseStateLatest,ForecastPublicArea,MeasuredBuildingArea,
                        FROM HouseInfoItem WHERE City='乌鲁木齐' 
                        AND RecordTime>'{0}' AND HouseID !=''
                        AND HouseState ='可售' 
                        AND HouseStateLatest ='不可售'
                        ORDER BY RecordTime DESC ) AS col 
                        Group BY col.HouseID LIMIT 3)tmp'''.format(searchtime)}
    optionSave = {'dbtable':"deal_case_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,CaseCore)).toDF().drop(*dropLT)
    result = savePub(cleandf,optionSave)
    print ('dealcase'+result)

def supplyCaseClean(searchtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('supplyCase script is going run')
    dropLT =['HouseStateLatest','ExtraJson']
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = {'dbtable':'''(SELECT * FROM 
                        (SELECT SourceUrl as SourceLink,
                        RecordTime as CaseTime,
                        FloorName as ActualFloor,
                        HouseState as SellState,
                        DistrictName,ProjectName,BuildingName,
                        IsMortgage,IsAttachment,IsPrivateUse,IsMoveBack,
                        MeasuredInsideOfBuildingArea,MeasuredSharedPublicArea,
                        IsSharedPublicMatching,BuildingStructure,SellSchedule,
                        UnitShape,UnitStructure,Balconys,UnenclosedBalconys,
                        HouseName,HouseNumber,TotalPrice,Price,PriceType,Address,
                        FloorName,HouseUseType,Dwelling,Remarks,RecordTime,
                        ProjectUUID,BuildingUUID,HouseUUID,UnitUUID,ExtraJson
                        BuildingID,HouseID,ForecastBuildingArea,ForecastInsideOfBuildingArea,
                        RealEstateProjectID,HouseStateLatest,ForecastPublicArea,MeasuredBuildingArea,
                        FROM HouseInfoItem WHERE City='乌鲁木齐' 
                        AND RecordTime>'{0}' AND HouseID !=''
                        AND HouseState ='可售 
                        AND HouseStateLatest =''
                        ORDER BY RecordTime DESC ) AS col 
                        Group BY col.HouseID LIMIT 3)tmp'''.format('1970-01-01'),
                        }
    optionSave = {'dbtable':"supply_case_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,CaseCore)).toDF().drop(*dropLT)
    result = savePub(cleandf,optionSave)
    print ('supplyCase'+result)

def quitCaseClean(searchtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('supplyCase script is going run')
    dropLT =['HouseStateLatest','ExtraJson']
    sc = SparkSession.builder.appName("master").getOrCreate()
    optionRead = {'dbtable':'''(SELECT * FROM 
                        (SELECT SourceUrl as SourceLink,
                        RecordTime as CaseTime,
                        FloorName as ActualFloor,
                        HouseState as SellState,
                        DistrictName,ProjectName,BuildingName,
                        IsMortgage,IsAttachment,IsPrivateUse,IsMoveBack,
                        MeasuredInsideOfBuildingArea,MeasuredSharedPublicArea,
                        IsSharedPublicMatching,BuildingStructure,SellSchedule,
                        UnitShape,UnitStructure,Balconys,UnenclosedBalconys,
                        HouseName,HouseNumber,TotalPrice,Price,PriceType,Address,
                        FloorName,HouseUseType,Dwelling,Remarks,RecordTime,
                        ProjectUUID,BuildingUUID,HouseUUID,UnitUUID,ExtraJson
                        BuildingID,HouseID,ForecastBuildingArea,ForecastInsideOfBuildingArea,
                        RealEstateProjectID,HouseStateLatest,ForecastPublicArea,MeasuredBuildingArea,
                        FROM HouseInfoItem WHERE City='乌鲁木齐' 
                        AND RecordTime>'{0}' AND HouseID !=''
                        AND HouseState ='不可售'
                        AND HouseStateLatest = '可售' 
                        ORDER BY RecordTime DESC ) AS col 
                        Group BY col.HouseID LIMIT 3)tmp'''.format('1970-01-01'),
                        }
    optionSave = {'dbtable':"supply_case_wulumuqi"}
    df = readPub(sc,optionRead)
    cleandf = df.rdd.map(lambda r:coreCleanPub(r,CaseCore)).toDF().drop(*dropLT)
    result = savePub(cleandf,optionSave)
    print ('supplyCase'+result)

if __name__ == '__main__':
    searchTime = (datetime.datetime.now()\
                    -datetime.timedelete(days=1)).stftime('%Y-%m-%d')
    # projectClean()
    # buildingClean()
    # houseClean()
    # dealCaseClean()
    # supplyCaseClean()