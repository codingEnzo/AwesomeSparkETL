# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys

import demjson
import pandas as pd
from pyspark.sql import Row

from Utils import Meth

sys.path.append('/home/lin/Dev/AwesomeSparkETL/Src/SparkETLCore')

ENGINE = Meth.getEngine('spark_test')
METHODS = [
    'actualFloor', 'address', 'balconys', 'buildingId', 'buildingName',
    'buildingStructure', 'caseTime', 'city', 'decoration', 'decorationPrice',
    'districtName', 'dwelling', 'extraJSON', 'floorCount', 'floorName',
    'floorRight', 'floorType', 'forecastBuildingArea',
    'forecastInsideOfBuildingArea', 'forecastPublicArea', 'halls', 'houseId',
    'houseLabel', 'houseLabelLatest', 'houseName', 'houseNature',
    'houseNumber', 'houseSalePrice', 'houseShape', 'houseState',
    'houseStateLatest', 'houseType', 'houseUUID', 'houseUseType',
    'isAttachment', 'isMortgage', 'isMoveback', 'isPrivateUse',
    'isSharedPublicMatching', 'kitchens', 'measuredBuildingArea',
    'measuredInsideOfBuildingArea', 'measuredSharedPublicArea',
    'measuredUndergroundArea', 'natureOfPropertyRight', 'price', 'priceType',
    'projectName', 'realEstateProjectId', 'remarks', 'rooms',
    'salePriceByBuildingArea', 'salePriceByInsideOfBuildingArea',
    'sellSchedule', 'sellState', 'sourceUrl', 'toilets', 'totalPrice',
    'toward', 'unEnclosedBalconys', 'unitId', 'unitName', 'unitShape',
    'unitStructure'
]


def caseTime(data):
    return data


def projectName(data):
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    return data


def buildingId(data):
    return data


def city(data):
    data = data.asDict()
    data['City'] = '徐州'
    data = Row(**data)
    return data


def districtName(data):
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT ProjectInfoItem.DistrictName FROM ProjectInfoItem " \
        "WHERE ProjectInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _name = query.iloc[0]['DistrictName'] \
            .replace('金山桥经济开发区', '经济技术开发区')
        data['DistrictName'] = _name

    data = Row(**data)
    return data


def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    return data


def houseName(data):
    return data


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT ProjectInfoItem.ProjectAddress FROM ProjectInfoItem " \
        "WHERE ProjectInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _addr = query.iloc[0]['ProjectAddress']
        data['Address'] = _addr

    data = Row(**data)
    return data


def floorName(data):
    data = data.asDict()
    house_name = data['HouseName']
    print('house_name: ', house_name)
    floor_name = Meth.getFloor(house_name)
    data['FloorName'] = floor_name
    data['HouseNumber'] = house_name
    data['HouseName'] = data['unitName'] + '单元' \
        + floor_name + '层' + house_name
    data = Row(**data)
    return data


def actualFloor(data):
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
    data = data.asDict()
    _ = data['BuildingStructure']
    _ = _.replace('钢混', '钢混结构') \
        .replace('框架', '框架结构') \
        .replace('钢筋混凝土', '钢混结构') \
        .replace('混合', '混合结构') \
        .replace('结构结构', '结构') \
        .replace('砖混', '砖混结构') \
        .replace('框剪', '框架剪力墙结构') \
        .replace('钢、', '')
    data['BuildingStructure'] = _
    data = Row(**data)
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


def extraJSON(data):
    _ = demjson.encode(data)
    return _
