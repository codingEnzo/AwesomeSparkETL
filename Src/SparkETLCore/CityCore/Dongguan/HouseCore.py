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
  data['City'] = u'常州'
  return data


def districtName(data):
  # print(data, inspect.stack()[0][3])
  return data


def unitName(data):
  return data


def unitId(data):
  return data


def houseNumber(data):
  # print(data, inspect.stack()[0][3])
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
                   sql=u"select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                       projectName=data['ProjectName']))
  data['Address'] = df.col.values[-1] if not df.empty else ''
  # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
  return data


def floorName(data):
  # print(data, inspect.stack()[0][3])
  return data


def actualFloor(data):
  # print(data, inspect.stack()[0][3])
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
  data = data.asDict()
  data['ActualFloor'] = str(getFloor(data['HouseName']))
  return data


def floorCount(data):
  # print(data, inspect.stack()[0][3])
  return data


def floorType(data):
  return data


def floorHight(data):
  # print(data, inspect.stack()[0][3])
  return data


def unitShape(data):
  return data


def unitStructure(data):
  return data


def rooms(data):
  return data


def halls(data):
  return data


def kitchens(data):
  # print(data, inspect.stack()[0][3])
  return data


def toilets(data):
  # print(data, inspect.stack()[0][3])
  return data


def balconys(data):
  # print(data, inspect.stack()[0][3])
  return data


def unenclosedBalconys(data):
  # print(data, inspect.stack()[0][3])
  return data


def houseShape(data):
  return data


def dwelling(data):
  return data


def forecastBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  return data


def forecastInsideOfBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  return data


def forecastPublicArea(data):
  # print(data, inspect.stack()[0][3])
  return data


def measuredBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  data = data.asDict()
  data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
  return data


def measuredInsideOfBuildingArea(data):
  # print(data, inspect.stack()[0][3])
  return data


def measuredSharedPublicArea(data):
  # print(data, inspect.stack()[0][3])
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
  return data


def isAttachment(data):
  # print(data, inspect.stack()[0][3])
  return data


def isPrivateUse(data):
  # print(data, inspect.stack()[0][3])
  return data


def isMoveBack(data):
  # print(data, inspect.stack()[0][3])
  return data


def isSharedPublicMatching(data):
  # print(data, inspect.stack()[0][3])
  return data


def sellState(data):
  return data


def sellSchedule(data):
  return data


def houseState(data):
  return data


def houseStateLatest(data):
  return data


def houseLabel(data):
  return data


def houseLabelLatest(data):
  return data


def totalPrice(data):
  return data


def price(data):
  return data


def priceType(data):
  return data


def decorationPrice(data):
  return data


def remarks(data):
  return data


def sourceUrl(data):
  return data


def extraJson(data):
  return data
