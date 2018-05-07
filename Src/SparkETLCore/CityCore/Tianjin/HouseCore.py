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
           'unitStructure']


def recordTime(data):
  data = data.asDict()
  nowtime = str(datetime.datetime.now())
  if data['RecordTime'] == '':
    data['RecordTime'] = nowtime
  return data


def caseTime(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['CaseTime'] = str(datetime.datetime.now()
                         ) if data['CaseTime'] == '' else data['CaseTime']
  return data


def projectName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['ProjectName'] = Meth.cleanName(data['ProjectName'])
  return data


def realEstateProjectId(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['RealEstateProjectID'] = data['ProjectUUID']
  return data


def buildingName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['BuildingName'] = Meth.cleanName(data['BuildingName'])
  return data


def buildingId(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['BuildingID'] = data['BuildingUUID']
  return data


def city(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['City'] = u'天津'
  return data


def districtName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  df = pd.read_sql(con=Var.ENGINE,
                   sql=u"select DistrictName as col from ProjectInfoItem where City='天津' and ProjectName='{projectName}' order by RecordTime".format(
                       projectName=data['ProjectName']))
  data['DistrictName'] = df.col.values[-1] if not df.empty else ''
  return data


def unitName(data):
  return data


def unitId(data):
  return data


def houseNumber(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['HouseNumber'] = data['HouseNumber']
  return data


def houseName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['HouseName'] = data['HouseName']
  return data


def houseId(data):
  data = data.asDict()
  data['HouseID'] = data['HouseID']
  return data


def houseUUID(data):
  data = data.asDict()
  data['HouseUUID'] = data['HouseUUID']
  return data


def address(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  df = pd.read_sql(con=Var.ENGINE,
                   sql=u"select ProjectAddress as col from ProjectInfoItem where City='天津' and ProjectName='{projectName}' order by RecordTime".format(
                       projectName=data['ProjectName']))
  data['Address'] = df.col.values[-1] if not df.empty else ''
  # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
  return data


def floorName(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['FloorName'] = data['FloorName']
  return data


def actualFloor(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['ActualFloor'] = data['ActualFloor']
  return data


def floorCount(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['FloorCount'] = data['FloorCount']
  return data


def floorType(data):
  return data


def floorHight(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['FloorHight'] = data['FloorHight']
  return data


def unitShape(data):
    data = data.asDict()
    data['UnitShape'] = Meth.numberTable(data['UnitShape'])
    return data



def unitStructure(data):
    return data


def rooms(data):
    return data


def halls(data):
    return data


def kitchens(data):
    data = data.asDict()
    data['Kitchens'] = data['Kitchens']
    return data


def toilets(data):
    data = data.asDict()
    data['Toilets'] = data['Toilets']
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


def houseShape(data):
    return data


def dwelling(data):
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


def measuredUndergroundArea(data):
    return data


def toward(data):
    return data


def houseType(data):
    return data


def houseNature(data):
    return data


def decoration(data):
    return data


def natureOfPropertyRight(data):
    return data


def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['HouseUseType'] = data['HouseUseType']
    return data


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    return data


def houseSalePrice(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
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


def sellState(data):
    return data


def sellSchedule(data):
    return data


def houseState(data):
    data = data.asDict()
    data['HouseState'] = data['HouseState']
    return data


def houseStateLatest(data):
    data = data.asDict()
    data['HouseStateLatest'] = data['HouseStateLatest']

    return data


def houseLabel(data):
    data = data.asDict()
    data['HouseLabel'] = data['HouseLabel']
    return data


def houseLabelLatest(data):
    data = data.asDict()
    data['HouseLabelLatest'] = data['HouseLabelLatest']
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
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    data = data.asDict()
    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceUrl', ''))
    return data


def extraJson(data):
    return data
