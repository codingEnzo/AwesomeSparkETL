# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['actualFloor', 'address', 'balconys', 'buildingID', 'buildingName', 'buildingStructure', 'caseTime', 'city',
           'decoration', 'decorationPrice', 'districtName', 'dwelling', 'extraJSON', 'floorCount', 'floorName',
           'floorRight', 'floorType', 'forecastBuildingArea', 'forecastInsideOfBuildingArea', 'forecastPublicArea',
           'halls', 'houseId', 'houseLabel', 'houseLabelLatest', 'houseName', 'houseNature', 'houseNumber',
           'houseSalePrice', 'houseShape', 'houseState', 'houseStateLatest', 'houseType', 'houseUUID', 'houseUseType',
           'isAttachment', 'isMortgage', 'isMoveback', 'isPrivateUse', 'isSharedPublicMatching', 'kitchens',
           'measuredBuildingArea', 'measuredInsideOfBuildingArea', 'measuredSharedPublicArea',
           'measuredUndergroundArea', 'natureOfPropertyRight', 'price', 'priceType', 'projectID', 'projectName',
           'realEstateProjectId', 'recordtime', 'remarks', 'rooms', 'salePriceByBuildingArea',
           'salePriceByInsideOfBuildingArea', 'sellSchedule', 'sellState', 'sourceUrl', 'toilets', 'totalPrice',
           'toward', 'unEnclosedBalconys', 'unitId', 'unitName', 'unitShape', 'unitStructure']


def recordtime(data):
    return data


def caseTime(data):
    data['CaseTime'] = data['RecordTime']
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def projectID(data):
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingID(data):
    return data


def city(data):
    return data


def districtName(data):
    df = pd.read_sql(con=Var.ENGINE,
                     sql="SELECT DistrictName AS col FROM ProjectInfoItem WHERE ProjectUUID = '{0}' "
                         "AND DistrictName != '' LIMIT 1".format(data['ProjectUUID'])).fillna('')
    data['DistrictName'] = df.col.values[0] if not df.empty else ''
    return data


def unitName(data):
    data['UnitName'] = Meth.cleanName(data['UnitName'])
    return data


def unitId(data):
    return data


def houseNumber(data):
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return data


def houseName(data):
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return data


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    data['Address'] = Meth.cleanName(data['ProjectAddress'])
    return data


def floorName(data):
    data['FloorName'] = Meth.cleanName(data['FloorName'])
    return data


def actualFloor(data):
    if data['FloorName']:
        c = re.search('-?\d+', data['FloorName'])
        data['ActualFloor'] = c.group() if c else ''
    return data


def floorCount(data):
    return data


def floorType(data):
    return data


def floorRight(data):
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
    return data


def toilets(data):
    return data


def balconys(data):
    return data


def unEnclosedBalconys(data):
    return data


def houseShape(data):
    return data


def dwelling(data):
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
    return data


def buildingStructure(data):
    return data


def houseSalePrice(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['HouseSalePrice'])
    data['HouseSalePrice'] = c.group() if c else ''
    return data


def salePriceByBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['SalePriceByBuildingArea'])
    data['SalePriceByBuildingArea'] = c.group() if c else ''
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def isMortgage(data):
    return data


def isAttachment(data):
    return data


def isPrivateUse(data):
    return data


def isMoveback(data):
    return data


def isSharedPublicMatching(data):
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
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['HouseSalePrice'])
    data['TotalPrice'] = c.group() if c else ''
    return data


def price(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['SalePriceByBuildingArea'])
    data['Price'] = c.group() if c else ''
    return data


def priceType(data):
    data['PriceType'] = '预售方案备案单价'.decode('utf-8')
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    data['SourceUrl'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraHouseUrl', '')
    return data


def extraJSON(data):
    return data
