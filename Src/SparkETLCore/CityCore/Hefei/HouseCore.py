# coding=utf-8
from __future__ import division
import sys
import re
import os
import inspect
# sys.path.append(os.path.dirname(os.getcwd()))
from SparkETLCore.Utils import Meth, Config, Var

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
           'districtname',
           'dwelling',
           'extraJSON',
           'floorCount',
           'floorName',
           'floorRight',
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
           'isMoveback',
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
           'unEnclosedBalconys',
           'unitId',
           'unitName',
           'unitShape',
           'unitStructure']


def caseTime(data):
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(data):
    return data


def city(data):
    return data


def districtname(data):
    if data['DistrictName']:
        data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return data


def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return data


def houseName(data):
    data['HouseName'] = data['FloorName'] + Meth.cleanName(data['HouseNumber'])
    return data


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    if data['Address']:
        data['Address'] = Meth.cleanName(data['Address'])
    return data


def floorName(data):
    data['FloorName'] = data['FloorName'] + '层'
    return data


def actualFloor(data):
    data['ActualFloor'] = data['FloorName']
    return data


def floorCount(data):
    return data


def floorType(data):
    return data


def floorRight(data):
    return data


def unitShape(data):
    data['UnitShape'] = Meth.cleanName(
        Meth.jsonLoad(data['ExtraJson']).get('ExtraHouseType', ''))
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
    return data


def forecastInsideOfBuildingArea(data):
    return data


def forecastPublicArea(data):
    return data


def measuredBuildingArea(data):
    if data['MeasuredBuildingArea']:
        data['MeasuredBuildingArea'] = Meth.cleanUnit(
            data['MeasuredBuildingArea'])
    return data


def measuredInsideOfBuildingArea(data):
    if data['MeasuredInsideOfBuildingArea']:
        data['MeasuredInsideOfBuildingArea'] = Meth.cleanUnit(
            data['MeasuredInsideOfBuildingArea'])
    return data


def measuredSharedPublicArea(data):
    if data['MeasuredSharedPublicArea']:
        data['MeasuredSharedPublicArea'] = Meth.cleanUnit(
            data['MeasuredSharedPublicArea'])
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
    return data


def buildingStructure(data):
    if data['BuildingStructure']:
        data['BuildingStructure'] = Meth.cleanName(data['BuildingStructure']).replace('钢混', '钢混结构')\
            .replace('框架', '框架结构')\
            .replace('钢筋混凝土', '钢混结构')\
            .replace('混合', '混合结构')\
            .replace('结构结构', '结构')\
            .replace('砖混', '砖混结构')\
            .replace('框剪', '框架剪力墙结构')\
            .replace('钢、', '')
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
    price = Meth.cleanUnit(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraHousePreSellPrice', '')).replace('-', '')
    area = Meth.cleanUnit(data['MeasuredBuildingArea'])
    if price and area:
        try:
            data['TotalPrice'] = str(round(float(price) * float(area), 2))
        except Exception as e:
            data['TotalPrice'] = ''
    return data


def price(data):
    price = Meth.cleanUnit(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraHousePreSellPrice', '')).replace('-', '')
    data['Price'] = price
    return data


def priceType(data):
    price = Meth.cleanUnit(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraHousePreSellPrice', '')).replace('-', '')
    if price:
        data['PriceType'] = '备案价格'
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJSON(data):
    return data
