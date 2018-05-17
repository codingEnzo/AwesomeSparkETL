# coding=utf-8
from __future__ import division
import datetime
import re

from SparkETLCore.Utils import Meth

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
           'unitStructure',
           'projectUUID',
           'buildingUUID',
           'unitUUID']


def recordTime(data):
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(data):
    return data


def buildingUUID(data):
    return data


def unitUUID(data):
    return data


def caseTime(data):
    data['CaseTime'] = str(datetime.datetime.now()) if data['CaseTime'] == '' else data['CaseTime']
    return data


def projectName(data):
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(data):
    return data


def city(data):
    data['City'] = u'常州'
    return data


def districtName(data):
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
    return data


def floorName(data):
    return data


def actualFloor(data):
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

    data['ActualFloor'] = str(getFloor(data['HouseName']))
    return data


def floorCount(data):
    return data


def floorType(data):
    return data


def floorHight(data):
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


def unenclosedBalconys(data):
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


def isMoveBack(data):
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
    if data['Prince'] == '0.0':
        data['Prince'] = ''
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
