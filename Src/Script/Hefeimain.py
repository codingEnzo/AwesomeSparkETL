# coding:utf-8
import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# from pyspark import SparkContext
from sqlalchemy import *
sys.path.append(os.path.dirname(os.getcwd()))
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import PresellCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import CaseCore
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import ENGINE
import datetime


def readPub (sc,table,groupid):
    options.update({'url':'''jdbc:mysql://10.30.1.70:3307/spark_test?
                             useUnicode=true&characterEncoding=utf-8''',
                    'driver':"com.mysql.jdbc.Driver",
                    "user":"root","password":"gh001"
                    "dbtable":'''(SELECT * FROM 
                                 (SELECT * FROM {table} 
                                 WHERE City='合肥' AND {groupid} !=''
                                 ORDER BY RecordTime DESC ) AS col 
                                 Group BY col.{groupid} LIMIT 10) {table}'''\
                                 .format(table=options['table'], 
                                         groupid=options['groupid'])})
    df = sc.read.format("jdbc").options(**options).load().fillna('')
    df.createOrReplaceTempView(table)
    return df

def savePub (df,options):
    options.update({'url':'''jdbc:mysql://10.30.1.70:3307/spark_caches?
                            useUnicode=true&characterEncoding=utf-8''',
                    'driver':"com.mysql.jdbc.Driver",
                    "user":"root",
                    "password":"gh001"})
    df.write.format("jdbc").mode('append').options(**options).save()
    return 'finish'

def coreCleanPub (data,cls):
    data = data.asDict()
    for index,method in enumerate(cls.METHODS):
        func = getattr(cls,method)
        data = func(data)
    data = Row(**data)
    return data


def projectETL(sc,df):
    optionSave = {'dbtable':"project_info_hefei"}
    dropLT= ['HouseBuildingCount','PresalePermitNumber']
    p = proDF.alias('p').drop(*dropLT)
    b = sc.sql(
            '''SELECT ProjectUUID,
            count(distinct(BuildingName)) AS HouseBuildingCount
            concat_ws('@#$', collect_list(distinct PresalePermitNumber)) AS PresalePermitNumber
            concat_ws('@#$', collect_list(distinct BuildingArea)) AS BTotalBuidlingArea
            concat_ws('@#$', collect_list(distinct ExtraJson)) AS BuiExtraJson
            FROM BuildingInfoItem GROUP BY ProjectUUID''').alias(b)
    p = p.join(b2,'ProjectUUID','left')\
           .select(proDF.columns+\
                    [col('b.HouseBuildingCount'),
                    col('b.PresalePermitNumber')])
    cleandf = p.rdd.map(lambda r :coreCleanPub(r,ProjectCore)).toDF().select()
    result = savePub(cleandf,{'dbtable':"project_info_hefei"})

def buildingETL(sc,df):
    pass

def houseETL(sc,df):
    pass

def caseETL(sc,df):
    pass


def main():
    sc =SparkSession.builder.appName("Hefei")\
            .config('spark.cores.max',8)\
            .config('spark.sql.execution.arrow.enabled','true')\
            .config('spark.sql.codegen','true')\
            .config("spark.local.dir", "~/.tmp").getOrCreate()

    proDF = readPub(sc,table='ProjectInfoItem',groupid='ProjectID')
    buiDF = readPub(sc,table='BuildingInfoItem',groupid='BuildingID')
    # houDF = readPub(sc,table='HouseInfoItem',groupid='HouseID')

    ProjectETL(sc,proDF)
    # buildingETL(sc,buiDF)
    # houseETL(sc,houDF)
    # caseETL(sc,houDF)

if __name__ == '__main__':
    main()
    