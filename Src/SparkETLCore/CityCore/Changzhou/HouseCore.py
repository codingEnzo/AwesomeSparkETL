# coding=utf-8
from __future__ import division
import sys
import datetime
import inspect
import re
import pandas as pd
import numpy as np

from pyspark.sql import Row
from SparkETLCore.Utils import Meth, Var, Config

METHODS = ['actualFloor',
           'address',
           'balconys',
           'buildingId',
           'buildingName',
           'buildingStructure',
           'caseTime',
           'city',
           'decoration',
           'decorationPrice',
           'districtName',
           'dwelling',
           'extraJson',
           'floorCount',
           'floorHight',
           'floorName',
           'floorType',
           'forecastBuildingArea',
           'forecastInsideOfBuildingArea',
           'forecastPublicArea',
           'halls',
           'houseId',
           'houseLabel',
           'houseLabelLatest',
           'houseName',
           'houseNature',
           'houseNumber',
           'houseSalePrice',
           'houseShape',
           'houseState',
           'houseStateLatest',
           'houseType',
           'houseUUID',
           'houseUseType',
           'isAttachment',
           'isMortgage',
           'isMoveBack',
           'isPrivateUse',
           'isSharedPublicMatching',
           'kitchens',
           'measuredBuildingArea',
           'measuredInsideOfBuildingArea',
           'measuredSharedPublicArea',
           'measuredUndergroundArea',
           'natureOfPropertyRight',
           'price',
           'priceType',
           'projectName',
           'realEstateProjectId',
           'recordTime',
           'remarks',
           'rooms',
           'salePriceByBuildingArea',
           'salePriceByInsideOfBuildingArea',
           'sellSchedule',
           'sellState',
           'sourceUrl',
           'toilets',
           'totalPrice',
           'toward',
           'unenclosedBalconys',
           'unitId',
           'unitName',
           'unitShape',
           'unitStructure',
           'projectUUID',
           'buildingUUID',
           'unitUUID']


def recordTime(spark, data):
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(spark, data):
    return data


def buildingUUID(spark, data):
    return data


def unitUUID(spark, data):
    return data


def caseTime(spark, data):
    data['CaseTime'] = str(datetime.datetime.now()) if data['CaseTime'] == '' else data['CaseTime']
    return data


def projectName(spark, data):
    return data


def realEstateProjectId(spark, data):
    return data


def buildingName(spark, data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(spark, data):
    return data


def city(spark, data):
    data['City'] = u'常州'
    return data


def districtName(spark, data):
    return data


def unitName(spark, data):
    return data


def unitId(spark, data):
    return data


def houseNumber(spark, data):
    return data


def houseName(spark, data):
    return data


def houseId(spark, data):
    return data


def houseUUID(spark, data):
    return data


def address(spark, data):
    sql = u"select ProjectAddress as col from ProjectInfoItem where City='常州' and ProjectName='{projectName}' order by RecordTime".format(
        projectName=data['ProjectName'])
    df = spark.sql(sql).toPandas()
    data['Address'] = df.col.values[-1] if not df.empty else ''
    # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
    return data


def floorName(spark, data):
    return data


def actualFloor(spark, data):
    def getFloor(x):
        if x == '':
            return None
        x_match = re.search(r'(\d+)', x)
        if not x_match:
            return None
        if len(x_match.group(1)) <= 3:
            res = x_match.group(1)[0]
        else:
            res = x_match.group(1)[0:2]
        if x[0] == '-':
            res = '-' + res
        return res

    data['ActualFloor'] = str(getFloor(data['HouseName']))
    return data


def floorCount(spark, data):
    return data


def floorType(spark, data):
    return data


def floorHight(spark, data):
    return data


def unitShape(spark, data):
    return data


def unitStructure(spark, data):
    return data


def rooms(spark, data):
    return data


def halls(spark, data):
    return data


def kitchens(spark, data):
    return data


def toilets(spark, data):
    return data


def balconys(spark, data):
    return data


def unenclosedBalconys(spark, data):
    return data


def houseShape(spark, data):
    return data


def dwelling(spark, data):
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


def measuredUndergroundArea(spark, data):
    return data


def toward(spark, data):
    return data


def houseType(spark, data):
    return data


def houseNature(spark, data):
    return data


def decoration(spark, data):
    return data


def natureOfPropertyRight(spark, data):
    return data


def houseUseType(spark, data):
    data['HouseUseType'] = data['HouseUseType']
    return data


def buildingStructure(spark, data):
    return data


def houseSalePrice(spark, data):
    return data


def salePriceByBuildingArea(spark, data):
    return data


def salePriceByInsideOfBuildingArea(spark, data):
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


def sellState(spark, data):
    return data


def sellSchedule(spark, data):
    return data


def houseState(spark, data):
    return data


def houseStateLatest(spark, data):
    return data


def houseLabel(spark, data):
    return data


def houseLabelLatest(spark, data):
    return data


def totalPrice(spark, data):
    return data


def price(spark, data):
    return data


def priceType(spark, data):
    return data


def decorationPrice(spark, data):
    return data


def remarks(spark, data):
    return data


def sourceUrl(spark, data):
    return data


def extraJson(spark, data):
    return data
