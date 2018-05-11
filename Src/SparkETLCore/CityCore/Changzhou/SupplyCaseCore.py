# -*- coding: utf-8 -*-
# @Date    : 2018-04-26 10:37:06
# @Author  : Sun Jiajia (jiajia.sun@yunfangdata.com)
from __future__ import division
import sys
import datetime
import inspect
import re
import pandas as pd
import numpy as np

from pyspark.sql import Row
from SparkETLCore.Utils import Meth, Var, Config

METHODS = [
    'RecordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
    'houseId',
    'forecastBuildingArea',
    'forecastInsideOfBuildingArea',
    'forecastPublicArea',
    'measuredBuildingArea',
    'measuredInsideOfBuildingArea',
    'measuredSharedPublicArea',
    'isMortgage',
    'isAttachment',
    'isPrivateUse',
    'isMoveBack',
    'isSharedPublicMatching',
    'buildingStructure',
    'sellSchedule',
    'sellState',
    'sourceUrl',
    'caseTime',
    'caseFrom',
    'unitShape',
    'unitStructure',
    'balconys',
    'unenclosedBalconys',
    'districtName',
    'regionName',
    'projectName',
    'buildingName',
    'presalePermitNumber',
    'houseName',
    'houseNumber',
    'totalPrice',
    'price',
    'priceType',
    'address',
    'buildingCompletedYear',
    'ActualFloor',
    'FloorName',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'remarks',
]


def recordTime(spark, data):
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(spark, data):
    return data


def buildingUUID(spark, data):
    return data


def houseUUID(spark, data):
    return data


def houseId(spark, data):
    return data


def forecastBuildingArea(spark, data):
    return data


def forecastInsideOfBuildingArea(spark, data):
    return data


def forecastPublicArea(spark, data):
    return data


def measuredBuildingArea(spark, data):
    return data


def measuredInsideOfBuildingArea(spark, data):
    return data


def measuredSharedPublicArea(spark, data):
    return data


def isMortgage(spark, data):
    return data


def isAttachment(spark, data):
    return data


def isPrivateUse(spark, data):
    return data


def isMoveBack(spark, data):
    return data


def isSharedPublicMatching(spark, data):
    return data


def buildingStructure(spark, data):
    return data


def sellSchedule(spark, data):
    return data


def sellState(spark, data):
    return data


def sourceUrl(spark, data):
    return data


def caseTime(spark, data):
    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(spark, data):
    return data


def unitShape(spark, data):
    return data


def unitStructure(spark, data):
    return data


def balconys(spark, data):
    return data


def unenclosedBalconys(spark, data):
    return data


def districtName(spark, data):
    return data


def regionName(spark, data):
    sql = u"select RegionName as col from ProjectInfoItem where City='常州' and ProjectName='{projectName}' order by RecordTime".format(
        projectName=data['ProjectName'])
    df = spark.sql(sql).toPandas()
    data['RegionName'] = df.col.values[-1] if not df.empty else ''
    return data


def projectName(spark, data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def buildingName(spark, data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def presalePermitNumber(spark, data):
    data['PresalePermitNumber'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresalePermitNumber', ''))
    return data


def houseName(spark, data):
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return data


def houseNumber(spark, data):
    return data


def totalPrice(spark, data):
    return data


def price(spark, data):
    return data


def priceType(spark, data):
    data['PriceType'] = u'成交均价'
    return data


def address(spark, data):
    sql = "select ProjectAddress as col from ProjectInfoItem where City='常州' and ProjectName='{projectName}' order by RecordTime".format(
        projectName=data['ProjectName'])
    df = spark.sql(sql).toPandas()
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return data


def buildingCompletedYear(spark, data):
    return data


def ActualFloor(spark, data):
    def getFloor(x):
        if x == '':
            return 0
        x_match = re.search(r'(\d+)', x)
        if not x_match:
            return 0
        if len(x_match.group(1)) <= 3:
            res = int(x_match.group(1)[0])
        else:
            res = int(x_match.group(1)[0:2])
        if x[0] == '-':
            res = -res
        return res

    housefloor = getFloor(data['HouseName'])
    if housefloor == 0:
        data['Floor'] = None
    else:
        data['Floor'] = str(housefloor)
    return data


def FloorName(spark, data):
    return data


def floors(spark, data):
    def getFloor(x):
        if x == '':
            return 0
        x_match = re.search(r'(\d+)', x)
        if not x_match:
            return 0
        if len(x_match.group(1)) <= 3:
            res = int(x_match.group(1)[0])
        else:
            res = int(x_match.group(1)[0:2])
        if x[0] == '-':
            res = -res
        return res

    sql = "select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
        buildingUUID=data['BuildingUUID'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        df['ActualFloor'] = df['HouseName'].apply(getFloor)
        acttualfloor = df.ActualFloor.agg('max')
        if acttualfloor == 0:
            data['Floors'] = None
        else:
            data['Floors'] = str(acttualfloor)
    return data


def houseUseType(spark, data):
    data['HouseUseType'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraHouseUseType', ''))
    return data


def dwelling(spark, data):
    return data


def state(spark, data):
    data['State'] = u'明确供应'
    return data


def remarks(spark, data):
    return data
