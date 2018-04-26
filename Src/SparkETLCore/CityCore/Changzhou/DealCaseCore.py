# coding=utf-8
from __future__ import division
import sys
import datetime
import inspect
import re
import pandas as pd
import numpy as np

sys.path.append('/home/sun/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Meth, Var, Config

METHODS = [
    'realEstateProjectID',
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
    'sourceLink',
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
    'gloor',
    'nominalFloor',
    'floors',
    'houseUseType',
    'dwelling',
    'state',
    'dealType',
    'remarks',
]


def realEstateProjectID(data):
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectID']
    return Row(**data)


def buildingID(data):
    data = data.asDict()
    data['BuildingID'] = data['BuildingID']
    return Row(**data)


def houseID(data):
    data = data.asDict()
    data['HouseID'] = data['HouseID']
    return Row(**data)


def forecastBuildingArea(data):
    return data


def forecastInsideOfBuildingArea(data):
    return data


def forecastPublicArea(data):
    return data


def measuredBuildingArea(data):
    data = data.asDict()
    data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
    return Row(**data)


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


def sourceLink(data):
    return data


def caseTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['CaseTime'] = str(datetime.datetime.now()
                           ) if data['CaseTime'] == '' else data['CaseTime']
    return Row(**data)


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
    data = data.asDict()
    data['RegionName'] = data['RegionName']
    return Row(**data)


def projectName(data):
    data = data.asDict()
    data['ProjectName'] = data['ProjectName']
    return Row(**data)


def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = data['BuildingName']
    return Row(**data)


def presalePermitNumber(data):
    data = data.asDict()
    data['PresalePermitNumber'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresalePermitNumber', ''))
    return Row(**data)


def houseName(data):
    data = data.asDict()
    data['HouseName'] = data['HouseName']
    return Row(**data)


def houseNumber(data):
    return data


def totalPrice(data):
    return data


def price(data):
    return data


def priceType(data):
    data = data.asDict()
    data['PriceType'] = '成交均价'
    return Row(**data)


def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return Row(**data)


def buildingCompletedYear(data):
    return data


def floor(data):
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

    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    housefloor = getFloor(data['HouseName'])
    if housefloor == 0:
        data['Floor'] = None
    else:
        data['Floor'] = str(housefloor)
    return Row(**data)


def nominalFloor(data):
    return data


def floors(data):
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
        # print(data, inspect.stack()[0][3])

    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['HouseName'].apply(getFloor)
    acttualfloor = df.ActualFloor.agg('max')
    if acttualfloor == 0:
        data['Floors'] = None
    else:
        data['Floors'] = str(acttualfloor)
    return Row(**data)


def houseUseType(data):
    data = data.asDict()
    data['HouseUseType'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraHouseUseType', ''))
    return Row(**data)


def dwelling(data):
    return data


def state(data):
    data = data.asDict()
    if data['HouseState'] == '未备案':
        data['State'] = '明确成交'
    else:
        data['State'] = '历史成交'
    return data


def dealType(data):
    return data


def remarks(data):
    return data
