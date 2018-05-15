# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import inspect
import pandas as pd
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['actualFloor',
           'address',
           'balconys',
           'buildingID',
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
    data['CaseTime'] = data['RecordTime']
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
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
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
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
    
    data['HouseName'] = data['HouseName']
    return data


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    # print(data, inspect.stack()[0][3])

    data['Address'] = data['ProjectAddress']
    # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
    return data


def floorName(data):
    # print(data, inspect.stack()[0][3])
    
    data['FloorName'] = data['FloorName']
    return data


def actualFloor(data):
    # print(data, inspect.stack()[0][3])
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
    # print(data, inspect.stack()[0][3])
    
    data['UnitShape'] = data['UnitShape'].translate(Var.NUMTAB_TRANS)
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
    
    data['HouseShape'] = data['HouseShape'].replace('null', '')
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
    
    data['HouseUseType'] = Meth.cleanName(data['HouseUseType']) if data['HouseUseType'] != 'null' else ''
    return data


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    return data


def houseSalePrice(data):
    

    data['HouseSalePrice'] = data['BuildingAveragePrice']
    return data


def salePriceByBuildingArea(data):
    data['SalePriceByBuildingArea'] = data['BuildingAveragePrice']
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
    data['TotalPrice'] = data['BuildingAveragePrice']
    return data


def price(data):
    return data


def priceType(data):
    
    data['PriceType'] = '预售单价'
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJson(data):
    return data
