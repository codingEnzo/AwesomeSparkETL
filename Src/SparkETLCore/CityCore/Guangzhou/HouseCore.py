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
    data['CaseTime'] = str(datetime.datetime.now()) if data[
        'CaseTime'] == '' else data['CaseTime']
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data['RealEstateProjectID'] = data['ProjectUUID']
    return data


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingID'] = data['BuildingUUID']
    return data


def city(data):
    # print(data, inspect.stack()[0][3])
    data['City'] = '广州'
    return data


def districtName(data):
    # # print(data, inspect.stack()[0][3])
    return data


def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    # print(data, inspect.stack()[0][3])
    data['HouseNumber'] = data['HouseNumber']
    return data


def houseName(data):
    # print(data, inspect.stack()[0][3])
    data['HouseName'] = data['HouseNumber']
    return data


def houseId(data):
    data['HouseID'] = data['HouseID']
    return data


def houseUUID(data):
    data['HouseUUID'] = data['HouseUUID']
    return data


def address(data):
    # # print(data, inspect.stack()[0][3])
    return data


def floorName(data):
    # print(data, inspect.stack()[0][3])
    data['FloorName'] = data['FloorName']
    return data


def actualFloor(data):
    # print(data, inspect.stack()[0][3])
    data['ActualFloor'] = data['ActualFloor']
    return data


def floorCount(data):
    # print(data, inspect.stack()[0][3])
    data['FloorCount'] = data['FloorCount']
    return data


def floorType(data):
    return data


def floorHight(data):
    # print(data, inspect.stack()[0][3])
    data['FloorHight'] = data['FloorHight']
    return data


def unitShape(data):
    def translate(x):
        for key in Var.NUMTAB:
            x = x.replace(key, Var.NUMTAB[key])
        return x

    # print(data, inspect.stack()[0][3])
    data['UnitShape'] = translate(data['UnitShape'])
    return data


def unitStructure(data):
    return data


def rooms(data):
    return data


def halls(data):
    return data


def kitchens(data):
    # print(data, inspect.stack()[0][3])
    data['Kitchens'] = data['Kitchens']
    return data


def toilets(data):
    # print(data, inspect.stack()[0][3])
    data['Toilets'] = data['Toilets']
    return data


def balconys(data):
    # print(data, inspect.stack()[0][3])
    data['Balconys'] = data['Balconys']
    return data


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])
    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return data


def houseShape(data):
    return data


def dwelling(data):
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
    data['HouseUseType'] = data['HouseUseType']
    return data


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构')\
        .replace('框架', '框架结构')\
        .replace('钢筋混凝土', '钢混结构')\
        .replace('混合', '混合结构')\
        .replace('结构结构', '结构')\
        .replace('砖混', '砖混结构')\
        .replace('框剪', '框架剪力墙结构')\
        .replace('钢，', '')
    return data


def houseSalePrice(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def isMortgage(data):
    # print(data, inspect.stack()[0][3])
    data['IsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMortgage', '')
    return data


def isAttachment(data):
    # print(data, inspect.stack()[0][3])
    data['IsAttachment'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsAttachment', '')
    return data


def isPrivateUse(data):
    # print(data, inspect.stack()[0][3])
    data['IsPrivateUse'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsPrivateUse', '')
    return data


def isMoveBack(data):
    # print(data, inspect.stack()[0][3])
    data['IsMoveBack'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMoveBack', '')
    return data


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])
    data['IsSharedPublicMatching'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsSharedPublicMatching', '')
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
