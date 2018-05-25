# coding=utf-8
from __future__ import division
import datetime

from SparkETLCore.Utils import Meth

METHODS = [
    'recordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
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
    'ActualFloor',
    'FloorName',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'dealType',
    'remarks',
    'unitUUID',
]


def unitUUID(data):
    return data


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
    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceUrl', ''))
    return data


def caseTime(data):
    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(data):
    return data


def unitShape(data):
    data['UnitShape'] = Meth.numberTable(data['UnitShape'])
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
    return data


def buildingName(data):
    return data


def presalePermitNumber(data):
    return data


def houseName(data):
    return data


def houseNumber(data):
    return data


def totalPrice(data):
    data['TotalPrice'] = data['TotalPrice'].replace(",", "")
    return data


def price(data):
    data['Price'] = data['Price'].replace(",", "")
    return data


def priceType(data):
    data['PriceType'] = u'成交均价'
    return data


def address(data):
    return data


def buildingCompletedYear(data):
    return data


def ActualFloor(data):
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
    data['State'] = u'明确退房'
    return data


def remarks(data):
    return data


def dealType(data):
    return data
