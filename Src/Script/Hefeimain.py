# coding:utf-8
import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
# from pyspark import SparkContext
from sqlalchemy import *
from pyspark.sql.types import *
sys.path.append(os.path.dirname(os.getcwd()))
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import PresellCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import CaseCore
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import ENGINE
import datetime


def projectClean(seaechtime='1970-01-01'):
    print ('project script is going run')
    def enterCore(data):
        for index,method in enumerate(ProjectCore.METHODS):
            func = getattr(ProjectCore,method)
            data = func(data)
        return data

    optionFields = {'url':"jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
           'driver':"com.mysql.jdbc.Driver",
           'dbtable':'''(SELECT * FROM 
                (SELECT * FROM ProjectInfoItem 
                WHERE City='合肥' AND RecordTime>'{0}'AND ProjectID !=''
                ORDER BY RecordTime DESC ) AS col 
                Group BY col.ProjectID) tmp'''.format(seaechtime),
           "user":"root",
           "password":"gh001"}
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = sc.read.format("jdbc").options(**optionFields).load()
    cleandf = df.rdd.map(lambda r:enterCore(r)).toDF()
    optionFields.update(
            {'url':"jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf-8",
            'dbtable':"project_info_hefei"})
    cleandf.write.format("jdbc").mode('append').options(**optionFields).save()
    print ('project script has finished')

def buildingClean(seaechtime='1970-01-01'):
    print ('building script is going run')
    def enterCore(data):
        # print(data)
        for index,method in enumerate(BuildingCore.METHODS):
            func = getattr(BuildingCore,method)
            data = func(data)
        return data
    optionFields = {'url':"jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
           'driver':"com.mysql.jdbc.Driver",
           'dbtable':'''(SELECT * FROM 
                    (SELECT * FROM BuildingInfoItem 
                    WHERE City='合肥' AND RecordTime>'{0}'AND BuildingID !=''
                    ORDER BY RecordTime DESC ) AS col 
                    Group BY col.BuildingID LIMIT 10)tmp'''.format(seaechtime),
           "user":"root",
           "password":"gh001",'model':'append'}
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = sc.read.format("jdbc").options(**optionFields).load()
    cleandf = df.rdd.map(lambda r:enterCore(r)).toDF()
    optionFields.update(
            {'url':"jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf-8",
            'dbtable':"building_info_hefei"})
    cleandf.write.format("jdbc").mode('append').options(**optionFields).save()

    print ('building script has finished')

def permitClean(seaechtime='1970-01-01'):
    #没有预售证表格不分离数据
    pass

def houseClean(seaechtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('house script is going run')
    def enterCore(data):
        # print(data)
        for index,method in enumerate(HouseCore.METHODS):
            func = getattr(HouseCore,method)
            data = func(data)
        return data
        # return ProjectCore.test(data)
    optionFields = {'url':"jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
           'driver':"com.mysql.jdbc.Driver",
           # 'dbtable':'''(SELECT * FROM 
           #          (SELECT * FROM HouseInfoItem 
           #          WHERE City='合肥' AND RecordTime>'{0}'AND HouseID !=''
           #          ORDER BY RecordTime DESC ) AS col 
           #          Group BY col.HouseID LIMIT 4)tmp'''.format(seaechtime),
           'dbtable':'''(SELECT * FROM HouseInfoItem 
                    WHERE HouseUUID ='0001de15-6b90-3d63-b3ab-88c25e40e678')tmp''',
           "user":"root",
           "password":"gh001",'model':'append'}
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = sc.read.format("jdbc").options(**optionFields).load()
    cleandf = df.rdd.map(lambda r:enterCore(r)).toDF()
    optionFields.update(
            {'url':"jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf-8",
            'dbtable':"house_info_hefei"})
    cleandf.write.format("jdbc").mode('append').options(**optionFields).save()
    print ('house script has finished')

def dealCaseClean(seaechtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('dealcase script is going run')
    def enterCore(data):
        for index,method in enumerate(CaseCore.METHODS):
            func = getattr(CaseCore,method)
            data = func(data)
        return data
        # return ProjectCore.test(data)
    dropLT =['HouseStateLatest','ExtraJson']    
    optionFields = {'url':"jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
           'driver':"com.mysql.jdbc.Driver",
           'dbtable':'''(SELECT * FROM 
                        (SELECT SourceUrl as SourceLink,
                        RecordTime as CaseTime,
                        FloorName as ActualFloor,
                        HouseState as SellState,
                        RealEstateProjectID,HouseStateLatest,
                        BuildingID,HouseID,ForecastBuildingArea,ForecastInsideOfBuildingArea,
                        ForecastPublicArea,MeasuredBuildingArea,
                        MeasuredInsideOfBuildingArea,MeasuredSharedPublicArea,
                        IsMortgage,IsAttachment,IsPrivateUse,IsMoveBack,
                        IsSharedPublicMatching,BuildingStructure,SellSchedule,
                        UnitShape,UnitStructure,Balconys,
                        UnenclosedBalconys,DistrictName,ProjectName,BuildingName,
                        HouseName,HouseNumber,TotalPrice,Price,PriceType,Address,
                        FloorName,HouseUseType,Dwelling,Remarks,RecordTime,
                        ProjectUUID,BuildingUUID,HouseUUID,UnitUUID,ExtraJson
                        FROM HouseInfoItem WHERE City='合肥' 
                        AND RecordTime>'{0}' AND HouseID !=''
                        AND HouseState in ("可售","抵押可售","摇号销售","现房销售") 
                        AND HouseStateLatest in ("现房销售","已签约","已备案","已办产权","网签备案单")
                        ORDER BY RecordTime DESC ) AS col 
                        Group BY col.HouseID LIMIT 3)tmp'''.format('1970-01-01'),
           "user":"root",
           "password":"gh001"}
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = sc.read.format("jdbc").options(**optionFields).load()
    cleandf = df.rdd.map(lambda r:enterCore(r)).toDF().drop(*dropLT)
    optionFields.update(
            {'url':"jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf-8",
            'dbtable':"deal_case_hefei"})
    cleandf.write.format("jdbc").mode('append').options(**optionFields).save()
    print ('dealcase script has finished')

def supplyCaseClean(seaechtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('supplyCase script is going run')
    def enterCore(data):
        for index,method in enumerate(CaseCore.METHODS):
            func = getattr(CaseCore,method)
            data = func(data)
        return data
        # return ProjectCore.test(data)
    # sc = SparkSession.builder.appName("master").getOrCreate()
    dropLT =['HouseStateLatest','ExtraJson']    
    optionFields = {'url':"jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
           'driver':"com.mysql.jdbc.Driver",
           'dbtable':'''(SELECT * FROM 
                        (SELECT SourceUrl as SourceLink,
                        RecordTime as CaseTime,
                        FloorName as ActualFloor,
                        HouseState as SellState,
                        RealEstateProjectID,HouseStateLatest,
                        BuildingID,HouseID,ForecastBuildingArea,ForecastInsideOfBuildingArea,
                        ForecastPublicArea,MeasuredBuildingArea,
                        MeasuredInsideOfBuildingArea,MeasuredSharedPublicArea,
                        IsMortgage,IsAttachment,IsPrivateUse,IsMoveBack,
                        IsSharedPublicMatching,BuildingStructure,SellSchedule,
                        UnitShape,UnitStructure,Balconys,
                        UnenclosedBalconys,DistrictName,ProjectName,BuildingName,
                        HouseName,HouseNumber,TotalPrice,Price,PriceType,Address,
                        FloorName,HouseUseType,Dwelling,Remarks,RecordTime,
                        ProjectUUID,BuildingUUID,HouseUUID,UnitUUID,ExtraJson
                        FROM HouseInfoItem WHERE City='合肥' 
                        AND RecordTime>'{0}' AND HouseID !=''
                        AND HouseState in ('可售','抵押可售','摇号销售','现房销售') 
                        AND HouseStateLatest =''
                        ORDER BY RecordTime DESC ) AS col 
                        Group BY col.HouseID LIMIT 3)tmp'''.format('1970-01-01'),
           "user":"root",
           "password":"gh001"}
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = sc.read.format("jdbc").options(**optionFields).load()
    cleandf = df.rdd.map(lambda r:enterCore(r)).toDF().drop(*dropLT)
    optionFields.update(
            {'url':"jdbc:mysql://10.30.1.70:3307/spark_caches?useUnicode=true&characterEncoding=utf-8",
            'dbtable':"supply_case_hefei"})
    cleandf.write.format("jdbc").mode('append').options(**optionFields).save()
    print ('supplycase script has finished')
    # print ('supplyCase script has finished')

if __name__ == '__main__':
    # projectClean()
    # buildingClean()
    # houseClean()
    # dealCaseClean()
    supplyCaseClean()