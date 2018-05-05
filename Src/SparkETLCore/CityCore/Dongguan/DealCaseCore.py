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
    'RecordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
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
    'ActualFloor',
    'FloorName',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'dealType',
    'remarks',
]
def recordTime(data):
    data = data.asDict()
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(data):
    data = data.asDict()
    data['ProjectID'] = data['RealEstateProjectID']
    return data


def buildingUUID(data):
    data = data.asDict()
    data['BuildingID'] = data['BuildingID']
    return data


def houseUUID(data):
    data = data.asDict()
    data['HouseUUID'] = data['HouseUUID']
    return data


def houseID(data):
    data = data.asDict()
    data['HouseID'] = data['HouseID']
    return data


def forecastBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['ForecastBuildingArea'] = data['ForecastBuildingArea']
  return data


def forecastInsideOfBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
  return data


def forecastPublicArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['ForecastPublicArea'] = data['ForecastPublicArea']
  return data


def measuredBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
  return data


def measuredInsideOfBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
  return data


def measuredSharedPublicArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
  return data



def isMortgage(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['IsMortgage'] = data['IsMortgage']
  return data


def isAttachment(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['IsAttachment'] = data['IsAttachment']
  return data


def isPrivateUse(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['IsPrivateUse'] = data['IsPrivateUse']
  return data


def isMoveBack(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['IsMoveBack'] = data['IsMoveBack']
  return data


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsSharedPublicMatching'] = data['IsSharedPublicMatching']
    return data


def buildingStructure(data):
    return data


def sellSchedule(data):
    return data


def sellState(data):
    return data


def sourceUrl(data):
    data = data.asDict()
    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceUrl', ''))
    return data


def caseTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(data):
    return data


def unitShape(data):
    data = data.asDict()
    data['UnitShape'] = Meth.numberTable(data['UnitShape'])
    return data


def unitStructure(data):
    return data


def balconys(data):
    data = data.asDict()
    data['Balconys'] = data['Balconys']
    return data


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return data


def districtName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  df = pd.read_sql(con=Var.ENGINE,
                   sql=u"select DistrictName as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                       projectName=data['ProjectName']))
  data['DistrictName'] = df.col.values[-1] if not df.empty else ''
  return data


def regionName(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select RegionName as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(projectName=data['ProjectName']))
    data['RegionName'] = df.col.values[-1] if not df.empty else ''
    return data


def projectName(data):
    data = data.asDict()
    data['ProjectName'] = data['ProjectName']
    return data


def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = data['BuildingName']
    return data


def presalePermitNumber(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select PresalePermitNumber as col from ProjectInfoItem where City = '东莞' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['PresalePermitNumber'] = df.col.values[-1] if not df.empty else ''
    return data


def houseName(data):
    data = data.asDict()
    data['HouseName'] = data['HouseName']
    return data


def houseNumber(data):
    data = data.asDict()
    data['HouseNumber'] = data['HouseNumber']
    return data


def totalPrice(data):
    data = data.asDict()
    data['TotalPrice'] = data['TotalPrice'].replace(",", "")
    return data


def price(data):
    data = data.asDict()
    data['Price'] = data['Price'].replace(",", "")
    return data


def priceType(data):
    data = data.asDict()
    data['PriceType'] = u'成交均价'
    return data


def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select ProjectAddress as col from ProjectInfoItem where City = '东莞' and ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return data


def buildingCompletedYear(data):
    return data


def ActualFloor(data):
    data = data.asDict()
    data['ActualFloor'] = data['ActualFloor']
    return data


def FloorName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorName'] = data['FloorName']
    return data


def floors(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select Floors as col from BuildingInfoItem where City = '东莞' and ProjectName='{projectName}' order by RecordTime".format(
                         BuildingName=data['BuildingName']))
    data['Floors'] = df.col.values[-1] if not df.empty else ''
    return data


def houseUseType(data):
    data = data.asDict()
    data['HouseUseType'] = data['HouseUseType']
    return data


def dwelling(data):
    return data


def state(data):
    data = data.asDict()
    if data['HouseSaleStateLatest'] == u'可售':
        data['State'] = u'明确成交'
    else:
        data['State'] = u'历史成交'
    return data


def dealType(data):
    return data


def remarks(data):
    return data
