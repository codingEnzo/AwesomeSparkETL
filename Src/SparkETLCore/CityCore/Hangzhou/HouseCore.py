# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')
sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

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
    data = data.asDict()
    data['CaseTime'] = Meth.jsonLoad(
            data['ExtraJson']).get('ExtraCurTimeStamp', '')
    return Row(**data)


def projectName(data):
    data = data.asDict()
    if data['ProjectName']:
        data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    else:
        df = pd.read_sql(con = Var.ENGINE,
                         sql = "select ProjectName as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and "
                               "ProjectName != '' ".format(
                                 projectUUID = data['ProjectUUID']))
        data['ProjectName'] = df.col.values[0]
    return Row(**data)


def projectID(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = " SELECT ProjectID as col FROM ProjectInfoItem WHERE ProjectUUID = '{projectUUID}'  AND ProjectID !='' LIMIT 1 ".format(
                         projectUUID = data['ProjectUUID']))

    data['ProjectID'] = df.col.values[0] if not df.empty else ''
    return Row(**data)


def realEstateProjectId(data):
    data = data.asDict()
    data['RealEstateProjectID'] = str(Meth.jsonLoad(
            data['ExtraJson']).get('ExtraProjectID', ''))
    return Row(**data)


def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingID(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "SELECT BuildingID as col FROM BuildingInfoItem WHERE BuildingUUID = {buildingUUID} limit 1" \
                     .format(buildingUUID = data['BuildingUUID']))
    data['BuildingID'] = df.col.values[0] if not df.empty else ''
    return Row(**data)


def city(data):
    return data


def districtName(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "SELECT DistrictName AS col FROM ProjectInfoItem WHERE ProjectUUID = '{0}' "
                           "AND DistrictName != '' LIMIT 1".format(data['ProjectUUID'])).fillna('')
    data['DistrictName'] = df.col.values[0] if not df.empty else ''
    return Row(**data)


def unitName(data):
    data = data.asDict()
    data['UnitName'] = Meth.cleanName(data['UnitName'])
    return Row(**data)


def unitId(data):
    return data


def houseNumber(data):
    data = data.asDict()
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return Row(**data)


def houseName(data):
    data = data.asDict()
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return Row(**data)


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    data = data.asDict()
    data['Address'] = Meth.cleanName(data['Address'])
    return Row(**data)


def floorName(data):
    return data


def actualFloor(data):
    data = data.asDict()
    if data['ActualFloor']:
        c = re.search('-?\d+', data['ActualFloor'])
        data['ActualFloor'] = c.group() if c else ''
    return Row(**data)


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
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastBuildingArea'])
    data['ForecastBuildingArea'] = c.group() if c else ''
    return Row(**data)


def forecastInsideOfBuildingArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastInsideOfBuildingArea'])
    data['ForecastInsideOfBuildingArea'] = c.group() if c else ''
    return Row(**data)


def forecastPublicArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastPublicArea'])
    data['ForecastPublicArea'] = c.group() if c else ''
    return Row(**data)


def measuredBuildingArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredBuildingArea'])
    data['MeasuredBuildingArea'] = c.group() if c else ''
    return Row(**data)


def measuredInsideOfBuildingArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredInsideOfBuildingArea'])
    data['MeasuredInsideOfBuildingArea'] = c.group() if c else ''
    return Row(**data)


def measuredSharedPublicArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredSharedPublicArea'])
    data['MeasuredSharedPublicArea'] = c.group() if c else ''
    return Row(**data)


def measuredUndergroundArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredUndergroundArea'])
    data['MeasuredUndergroundArea'] = c.group() if c else ''
    return Row(**data)


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
    return data


def salePriceByBuildingArea(data):
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
    data = data.asDict()
    data['TotalPrice'] = data['TotalPrice'].replace('元'.decode('utf-8'), '')
    return Row(**data)


def price(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['Price'])
    data['Price'] = c.group() if c else ''
    return Row(**data)


def priceType(data):
    data = data.asDict()
    data['PriceType'] = '备案价格'.decode('utf-8')
    return Row(**data)


def decorationPrice(data):
    data = data.asDict()
    data['DecorationPrice'] = data['DecorationPrice'].replace('元'.decode('utf-8'), '')
    return Row(**data)


def remarks(data):
    return data


def sourceUrl(data):
    data = data.asDict()
    data['SourceUrl'] = Meth.jsonLoad(
            data['ExtraJson']).get('ExtraHouseUrl', '')
    return Row(**data)


def extraJSON(data):
    return data
