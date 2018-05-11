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

METHODS = [
    'recordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
    'unitUUID',
    'houseID',
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
    'SourceUrl',
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
    'actualFloor',
    'floorName',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'dealType',
    'remarks',
]


def unitUUID(data):
    return data


def recordTime(data):

    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(data):

    data['ProjectUUID'] = data['RealEstateProjectID']
    return data


def buildingUUID(data):

    data['BuildingUUID'] = data['BuildingUUID']
    return data


def houseUUID(data):

    data['HouseUUID'] = data['HouseUUID']
    return data


def houseID(data):

    data['HouseID'] = data['HouseID']
    return data


def forecastBuildingArea(data):
    # print(data, inspect.stack()[0][3])

    data['ForecastBuildingArea'] = data['ForecastBuildingArea']
    return data


def forecastInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])

    data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
    return data


def forecastPublicArea(data):
    # print(data, inspect.stack()[0][3])

    data['ForecastPublicArea'] = data['ForecastPublicArea']
    return data


def measuredBuildingArea(data):
    # print(data, inspect.stack()[0][3])

    data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
    return data


def measuredInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])

    data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
    return data


def measuredSharedPublicArea(data):
    # print(data, inspect.stack()[0][3])

    data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
    return data


def isMortgage(data):
    # print(data, inspect.stack()[0][3])

    data['IsMortgage'] = data['IsMortgage']
    return data


def isAttachment(data):
    # print(data, inspect.stack()[0][3])

    data['IsAttachment'] = data['IsAttachment']
    return data


def isPrivateUse(data):
    # print(data, inspect.stack()[0][3])

    data['IsPrivateUse'] = data['IsPrivateUse']
    return data


def isMoveBack(data):
    # print(data, inspect.stack()[0][3])

    data['IsMoveBack'] = data['IsMoveBack']
    return data


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])

    data['IsSharedPublicMatching'] = data['IsSharedPublicMatching']
    return data


def buildingStructure(data):
    return data


def sellSchedule(data):
    return data


def sellState(data):
    return data


def sourceUrl(data):

    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceUrl', ''))
    return data


def caseTime(data):
    # print(data, inspect.stack()[0][3])

    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(data):
    return data


def unitShape(data):

    data['UnitShape'] = Meth.numberTable(data['UnitShape'])
    return data


def unitStructure(data):
    return data


def balconys(data):

    data['Balconys'] = data['Balconys']
    return data


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])

    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return data


def districtName(data):
    # print(data, inspect.stack()[0][3])

    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select DistrictName as col from ProjectInfoItem where City='天津' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['DistrictName'] = df.col.values[-1] if not df.empty else ''
    return data


def regionName(data):

    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select RegionName as col from ProjectInfoItem where City='天津' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['RegionName'] = df.col.values[-1] if not df.empty else ''
    return data


def projectName(data):

    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def buildingName(data):

    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def presalePermitNumber(data):

    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select PresalePermitNumber as col from ProjectInfoItem where City = '天津' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['PresalePermitNumber'] = df.col.values[-1] if not df.empty else ''
    return data


def houseName(data):

    data['HouseName'] = data['HouseName']
    return data


def houseNumber(data):

    data['HouseNumber'] = data['HouseNumber']
    return data


def totalPrice(data):

    data['TotalPrice'] = data['TotalPrice'].replace(",", "")
    return data


def price(data):

    data['Price'] = data['Price'].replace(",", "")
    return data


def priceType(data):

    data['PriceType'] = u'成交均价'
    return data


def address(data):

    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select ProjectAddress as col from ProjectInfoItem where City = '天津' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return data


def buildingCompletedYear(data):
    return data


def actualFloor(data):

    data['ActualFloor'] = data['ActualFloor']
    return data


def floorName(data):
    # print(data, inspect.stack()[0][3])

    data['FloorName'] = data['FloorName']
    return data


def floors(data):

    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select Floors as col from BuildingInfoItem where City = '天津' and ProjectName='{projectName}' order by RecordTime".format(
                         BuildingName=data['BuildingName']))
    data['Floors'] = df.col.values[-1] if not df.empty else ''
    return data


def houseUseType(data):

    data['HouseUseType'] = data['HouseUseType']
    return data


def dwelling(data):
    return data


def state(data):

    data['State'] = u'明确退房'
    return data


def remarks(data):
    return data
