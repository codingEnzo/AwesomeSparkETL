# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['actualFloor',
          'address',
          'balconys',
          'buildingCompletedYear',
          'buildingID',
          'buildingName',
          'buildingStructure',
          'buildingUUID',
          'caseFrom',
          'caseTime',
          'city',
          'dealType',
          'decoration',
          'decorationPrice',
          'districtName',
          'dwelling',
          'extraJson',
          'floorCount',
          'floorHight',
          'floorName',
          'floorType',
          'floors',
          'forecastBuildingArea',
          'forecastInsideOfBuildingArea',
          'forecastPublicArea',
          'halls',
          'houseID',
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
          'nominalFloor',
          'presalePermitNumber',
          'price',
          'priceType',
          'projectID',
          'projectName',
          'projectUUID',
          'realEstateProjectID',
          'recordTime',
          'regionName',
          'remarks',
          'rooms',
          'salePriceByBuildingArea',
          'salePriceByInsideOfBuildingArea',
          'sellSchedule',
          'sellState',
          'sourceUrl',
          'state',
          'toilets',
          'totalPrice',
          'toward',
          'unenclosedBalconys',
          'unitID',
          'unitName',
          'unitShape',
          'unitStructure',
          'unitUUID']


def actualFloor(data):
    c = re.search('-?\d+', data['FloorName'])
    data['ActualFloor'] = c.group() if c else ''
    return data


def address(data):
    data['Address'] = Meth.cleanName(data['ProjectAddress'])
    return data


def balconys(data):
    return data


def buildingCompletedYear(data):
    return data


def buildingID(data):
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingStructure(data):
    return data


def buildingUUID(data):
    return data


def caseFrom(data):
    return data


def caseTime(data):
    data['CaseTime'] = data['RecordTime']
    return data


def city(data):
    return data


def dealType(data):
    if data['HouseState'] =='已售' and data['HouseStateLatest'] == '可售':
        data['DealType'] = '明确成交'
    elif data['HouseState'] =='已售' and data['HouseStateLatest'] == '':
        data['DealType'] = '历史成交'
    return data


def decoration(data):
    return data


def decorationPrice(data):
    return data


def districtName(data):
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return data


def dwelling(data):
    return data


def extraJson(data):
    return data


def floorCount(data):
    return data


def floorHight(data):
    return data


def floorName(data):
    data['FloorName'] = Meth.cleanName(data['FloorName'])
    return data


def floorType(data):
    return data


def floors(data):
    data['Floors'] = str(data['Floors'])
    return data


def forecastBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastBuildingArea'])
    data['ForecastBuildingArea'] = c.group() if c else ''
    return data


def forecastInsideOfBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastInsideOfBuildingArea'])
    data['ForecastInsideOfBuildingArea'] = c.group() if c else ''
    return data


def forecastPublicArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastPublicArea'])
    data['ForecastPublicArea'] = c.group() if c else ''
    return data


def halls(data):
    return data


def houseID(data):
    return data


def houseLabel(data):
    return data


def houseLabelLatest(data):
    return data


def houseName(data):
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return data


def houseNature(data):
    return data


def houseNumber(data):
    return data


def houseSalePrice(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['HouseSalePrice'])
    data['HouseSalePrice'] = c.group() if c else ''
    return data


def houseShape(data):
    return data


def houseState(data):
    return data


def houseStateLatest(data):
    return data


def houseType(data):
    return data


def houseUUID(data):
    return data


def houseUseType(data):
    data['HouseUseType'] = Meth.cleanName(data['HouseUseType'])
    return data


def isAttachment(data):
    return data


def isMortgage(data):
    return data


def isMoveBack(data):
    return data


def isPrivateUse(data):
    return data


def isSharedPublicMatching(data):
    return data


def kitchens(data):
    return data


def measuredBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredBuildingArea'])
    data['MeasuredBuildingArea'] = c.group() if c else ''
    return data


def measuredInsideOfBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredInsideOfBuildingArea'])
    data['MeasuredInsideOfBuildingArea'] = c.group() if c else ''
    return data


def measuredSharedPublicArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredSharedPublicArea'])
    data['MeasuredSharedPublicArea'] = c.group() if c else ''
    return data


def measuredUndergroundArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredUndergroundArea'])
    data['MeasuredUndergroundArea'] = c.group() if c else ''
    return data


def natureOfPropertyRight(data):
    return data


def nominalFloor(data):
    data['NominalFloor'] = Meth.cleanName(data['FloorName'])
    return data


def presalePermitNumber(data):
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return data


def price(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['SalePriceByBuildingArea'])
    data['Price'] = c.group() if c else ''
    return data


def priceType(data):
    data['PriceType'] = '预售方案备案单价'
    return data


def projectID(data):
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def projectUUID(data):
    return data


def realEstateProjectID(data):
    return data


def recordTime(data):
    return data


def regionName(data):
    data['RegionName'] = Meth.cleanName(data['RegionName'])
    return data


def remarks(data):
    return data


def rooms(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def sellSchedule(data):
    return data


def sellState(data):
    data['SellState'] = data['HouseState']
    return data


def sourceUrl(data):
    return data


def state(data):
    if data['HouseState'] == '可售' and data['HouseStateLatest'] !='可售':
      data['State'] = '明确供应' 
    elif data['HouseState'] == '可售' and data['HouseStateLatest'] =='已售':
      data['State'] = '明确退房' 
    elif data['HouseStateLatest'] == '可售' and data['HouseState'] =='已售':
      data['State'] = '明确成交' 
    elif data['HouseStateLatest'] == '' and data['HouseState'] =='已售':
      data['State'] = '历史成交'
    return data


def toilets(data):
    return data


def totalPrice(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['HouseSalePrice'])
    data['TotalPrice'] = c.group() if c else ''
    return data


def toward(data):
    return data


def unenclosedBalconys(data):
    return data


def unitID(data):
    return data


def unitName(data):
    return data


def unitShape(data):
    return data


def unitStructure(data):
    return data


def unitUUID(data):
    return data
