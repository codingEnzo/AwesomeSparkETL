# coding: utf-8
import re
from SparkETLCore.Utils import Var, Meth, Config

METHODS = [
    'projectID',
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
    'dealType',
    'remarks',
    'recordTime',
    'projectUUID',
    'buildingUUID',
    'houseUUID',
    'unitUUID',
]


def projectID(data):
    return data


def buildingID(data):
    return data


def houseID(data):
    return data


def forecastBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastBuildingArea'])
    data['ForecastBuildingArea'] = c.group() if c else ''
    return data


def forecastInsideOfBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastInsideOfBuildingArea'])
    data['ForecastInsideOfBuildingArea'] = c.group() if c else ''
    return data


def forecastPublicArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastPublicArea'])
    data['ForecastPublicArea'] = c.group() if c else ''
    return data


def measuredBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredBuildingArea'])
    data['MeasuredBuildingArea'] = c.group() if c else ''
    return data


def measuredInsideOfBuildingArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredInsideOfBuildingArea'])
    data['MeasuredInsideOfBuildingArea'] = c.group() if c else ''
    return data


def measuredSharedPublicArea(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredSharedPublicArea'])
    data['MeasuredSharedPublicArea'] = c.group() if c else ''
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
    data['CaseTime'] = data['RecordTime']
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
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return data


def houseName(data):
    data['HouseName'] = Meth.cleanName(data['HouseName'])
    return data


def houseNumber(data):
    return data


def totalPrice(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['HouseSalePrice'])
    data['TotalPrice'] = c.group() if c else ''
    return data


def price(data):
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['SalePriceByBuildingArea'])
    data['Price'] = c.group() if c else ''
    return data


def priceType(data):
    data['PriceType'] = '预售方案备案单价'
    return data


def address(data):
    return data


def buildingCompletedYear(data):
    return data


def actualFloor(data):
    return data


def nominalFloor(data):
    data['NominalFloor'] = Meth.cleanName(data['FloorName'])
    return data


def floors(data):
    data['Floors'] = str(data['Floors'])
    return data


def houseUseType(data):
    data['HouseUseType'] = Meth.cleanName(data['HouseUseType'])
    return data


def dwelling(data):
    return data


def state(data):
    return data


def dealType(data):
    if '待售' in data['HouseStateLatest'] :
        data['DealType'] = '明确成交'
    elif data['HouseStateLatest'] == '':
        data['DealType'] = '历史成交'
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
