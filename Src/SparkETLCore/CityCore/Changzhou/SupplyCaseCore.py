# -*- coding: utf-8 -*-
from __future__ import division
import datetime
import re

from SparkETLCore.Utils import Meth

METHODS = [
    'RecordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
    'houseId',
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
    'ActualFloor',
    'FloorName',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'remarks',
]


def recordTime(data):
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectUUID(data):
    return data


def buildingUUID(data):
    return data


def houseUUID(data):
    return data


def houseId(data):
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
    return data


def isAttachment(data):
    return data


def isPrivateUse(data):
    return data


def isMoveBack(data):
    return data


def isSharedPublicMatching(data):
    return data


def buildingStructure(data):
    return data


def sellSchedule(data):
    return data


def sellState(data):
    return data


def sourceUrl(data):
    return data


def caseTime(data):
    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(data):
    return data


def unitShape(data):
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
    data['PresalePermitNumber'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectPresaleNumber', ''))
    return data


def houseName(data):
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return data


def houseNumber(data):
    return data


def totalPrice(data):
    return data


def price(data):
    if data['Price'] == '0.0':
        data['Price'] = ''
    return data


def priceType(data):
    data['PriceType'] = u'成交均价'
    return data


def address(data):
    return data


def buildingCompletedYear(data):
    return data


def ActualFloor(data):
    def getFloor(x):
        if x == '':
            return 0
        x_match = re.search(r'(\d+)', x)
        if not x_match:
            return 0
        if len(x_match.group(1)) <= 3:
            res = int(x_match.group(1)[0])
        else:
            res = int(x_match.group(1)[0:2])
        if x[0] == '-':
            res = -res
        return res

    housefloor = getFloor(data['HouseName'])
    if housefloor == 0:
        data['Floor'] = None
    else:
        data['Floor'] = str(housefloor)
    return data


def FloorName(data):
    return data


def floors(data):
    return data


def houseUseType(data):
    return data


def dwelling(data):
    return data


def state(data):
    data['State'] = u'明确供应'
    return data


def remarks(data):
    return data
