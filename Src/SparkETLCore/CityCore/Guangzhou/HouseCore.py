# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import sys
import datetime
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from SparkETLCore.Utils import Meth, Var, Config
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('guangzhou').getOrCreate()

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
    return data


def caseTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['CaseTime'] = str(datetime.datetime.now()) if data[
        'CaseTime'] == '' else data['CaseTime']
    return Row(**data)


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingID'] = data['BuildingUUID']
    return Row(**data)


def city(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['City'] = '广州'
    return Row(**data)


def districtName(data):
    # # print(data, inspect.stack()[0][3])
    data = data.asDict()
    # df = spark.sql("select DistrictName as col from projectinfoitem where ProjectName='{projectName}' limit 1".format(
    #     projectName=data['ProjectName'])).toPandas()
    # print('Check ProjectName')
    # data['DistrictName'] = df.col.values[-1] if not df.empty else ''
    # # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
    return Row(**data)


def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['HouseNumber'] = data['HouseNumber']
    return Row(**data)


def houseName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['HouseName'] = data['FloorName'] + data['HouseNumber']
    return Row(**data)


def houseId(data):
    data = data.asDict()
    data['HouseID'] = data['HouseID']
    return Row(**data)


def houseUUID(data):
    data = data.asDict()
    data['HouseUUID'] = data['HouseUUID']
    return Row(**data)


def address(data):
    # # print(data, inspect.stack()[0][3])
    data = data.asDict()
    # df = spark.sql("select ProjectAddress as col from projectinfoitem where ProjectName='{projectName}' limit 1".format(
    #     projectName=data['ProjectName'])).toPandas()
    # data['Address'] = df.col.values[-1] if not df.empty else ''
    # # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
    return Row(**data)


def floorName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorName'] = data['FloorName']
    return Row(**data)


def actualFloor(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ActualFloor'] = data['ActualFloor']
    return Row(**data)


def floorCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorCount'] = data['FloorCount']
    return Row(**data)


def floorType(data):
    return data


def floorHight(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorHight'] = data['FloorHight']
    return Row(**data)


def unitShape(data):
    def translate(x):
        for key in Var.NUMTAB:
            x = x.replace(key, Var.NUMTAB[key])
        return x

    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnitShape'] = translate(data['UnitShape'])
    return Row(**data)


def unitStructure(data):
    return data


def rooms(data):
    return data


def halls(data):
    return data


def kitchens(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Kitchens'] = data['Kitchens']
    return Row(**data)


def toilets(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Toilets'] = data['Toilets']
    return Row(**data)


def balconys(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Balconys'] = data['Balconys']
    return Row(**data)


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return Row(**data)


def houseShape(data):
    return data


def dwelling(data):
    return data


def forecastBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastBuildingArea'] = data['ForecastBuildingArea']
    return Row(**data)


def forecastInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
    return Row(**data)


def forecastPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastPublicArea'] = data['ForecastPublicArea']
    return Row(**data)


def measuredBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
    return Row(**data)


def measuredInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
    return Row(**data)


def measuredSharedPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
    return Row(**data)


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
    return Row(**data)


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构')\
        .replace('框架', '框架结构')\
        .replace('钢筋混凝土', '钢混结构')\
        .replace('混合', '混合结构')\
        .replace('结构结构', '结构')\
        .replace('砖混', '砖混结构')\
        .replace('框剪', '框架剪力墙结构')\
        .replace('钢，', '')
    return Row(**data)


def houseSalePrice(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def isMortgage(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMortgage', '')
    return Row(**data)


def isAttachment(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsAttachment'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsAttachment', '')
    return Row(**data)


def isPrivateUse(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsPrivateUse'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsPrivateUse', '')
    return Row(**data)


def isMoveBack(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsMoveBack'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMoveBack', '')
    return Row(**data)


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsSharedPublicMatching'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsSharedPublicMatching', '')
    return Row(**data)


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
