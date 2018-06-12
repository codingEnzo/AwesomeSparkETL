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

METHODS = ['projectID',
           'buildingID',
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
           'sourceUrl',
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
           'nominalFloor',
           'floors',
           'houseUseType',
           'dwelling',
           'state',
           'remarks',
           'recordTime',
           'projectUUID',
           'buildingUUID',
           'houseUUID',
           'unitUUID', ]


def projectID(data):
    return data


def buildingID(data):
    return data


def houseID(data):
    return data


def forecastBuildingArea(data):
    return data


def forecastInsideOfBuildingArea(data):
    return data


def forecastPublicArea(data):
    return data


def measuredBuildingArea(data):
    return data


def measuredInsideOfBuildingArea(data):
    return data


def measuredSharedPublicArea(data):
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


def buildingStructure(data):
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构')\
        .replace('框架', '框架结构')\
        .replace('钢筋混凝土', '钢混结构')\
        .replace('混合', '混合结构')\
        .replace('结构结构', '结构')\
        .replace('砖混', '砖混结构')\
        .replace('框剪', '框架剪力墙结构')\
        .replace('钢，', '')
    return data


def sellSchedule(data):
    return data


def sellState(data):
    data['SellState'] = data['HouseState']
    return data


def sourceUrl(data):
    return data


def caseTime(data):
    data['CaseTime'] = data['RecordTime']
    return data


def caseFrom(data):
    data['CaseFrom'] = data['SourceUrl']
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


def balconys(data):
    return data


def unenclosedBalconys(data):
    return data


def districtName(data):
    return data


def regionName(data):
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def presalePermitNumber(data):
    return data


def houseName(data):
    data['HouseName'] = data['HouseNumber']
    return data


def houseNumber(data):
    return data


def totalPrice(data):
    return data


def price(data):
    return data


def priceType(data):
    data['PriceType'] = '成交均价'
    return data


def address(data):
    return data


def buildingCompletedYear(data):
    return data


def actualFloor(data):
    return data


def nominalFloor(data):
    return data


def floors(data):
    data['Floors'] = data['FloorCount'].replace('层', '')
    return data


def houseUseType(data):
    return data


def dwelling(data):
    return data


def state(data):
    data['State'] = "明确供应"
    return data


def remarks(data):
    return data


def recordTime(data):
    return data


def projectUUID(data):
    return data


def buildingUUID(data):
    return data


def houseUUID(data):
    return data


def unitUUID(data):
    return data
