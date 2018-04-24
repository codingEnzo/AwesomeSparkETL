# coding=utf-8
from __future__ import division
import re
import sys
import inspect
import pandas as pd
import numpy as np
import datetime
sys.path.append('/home/sun/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

METHODS = ['address',
           'buildingArea',
           'buildingAveragePrice',
           'buildingCategory',
           'buildingHeight',
           'buildingId',
           'buildingName',
           'buildingPriceRange',
           'buildingStructure',
           'buildingType',
           'buildingUUID',
           'elevaltorInfo',
           'elevatorHouse',
           'estimatedCompletionDate',
           'extrajson',
           'floors',
           'housingCount',
           'isHasElevator',
           'onTheGroundFloor',
           'presalePermitNumber',
           'projectName',
           'realEstateProjectId',
           'recordTime',
           'remarks',
           'sourceUrl',
           'theGroundFloor',
           'unitId',
           'unitName',
           'units',
           'unsoldAmount']


def recordTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return Row(**data)


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingID'] = data['BuildingID']
    return Row(**data)


def buildingUUID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingUUID'] = data['BuildingUUID']
    return Row(**data)


def unitName(data):
    # print(data, inspect.stack()[0][3])
    return data


def unitId(data):
    # print(data, inspect.stack()[0][3])
    return data


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresalePermitNumber'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresalePermitNumber', ''))
    return Row(**data)


def address(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return Row(**data)


def onTheGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    def getFloor(x):
        if x=='':
            return 1
        if x[0] != '-':
            x_match = re.search(r'(\d+)', x)
            if not x_match:
                return 1
            if len(x_match.group(1)) <= 3:
                res = int(x_match.group(1)[0])
            else:
                res = int(x_match.group(1)[0:2])
        else:
            res = 1
        return res
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['HouseName'].apply(getFloor)
    data['OnTheGroundFloor'] = str(df.ActualFloor.agg('max'))
    return Row(**data)


def theGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    def getFloor(x):
        if x=='':
            return 0
        if x[0] == '-':
            x_match = re.search(r'(\d+)', x)
            if not x_match:
                return 0
            if len(x_match.group(1)) <= 3:
                res = -int(x_match.group(1)[0])
            else:
                res = -int(x_match.group(1)[0:2])
        else:
            res = 0
        return res
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['HouseName'].apply(getFloor)
    data['TheGroundFloor'] = str(df.ActualFloor.agg('max'))
    return Row(**data)


def estimatedCompletionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['TheGroundFloor'] = str(df.col.values[0])
    return Row(**data)


def floors(data):
    # print(data, inspect.stack()[0][3])
    return data


def elevatorHouse(data):
    # print(data, inspect.stack()[0][3])
    return data


def isHasElevator(data):
    # print(data, inspect.stack()[0][3])
    return data


def elevaltorInfo(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingType(data):
    def check_floor_type(floorname):
        if floorname <= 3:
            return '低层(1-3)'
        elif floorname <= 6:
            return '多层(4-6)'
        elif floorname <= 11:
            return '小高层(7-11)'
        elif floorname <= 18:
            return '中高层(12-18)'
        elif floorname <= 32:
            return '高层(19-32)'
        else:
            return '超高层(33)'
    # print(data, inspect.stack()[0][3])

    def getFloor(x):
        if x=='':
            return 1

        x_match = re.search(r'(\d+)', x)
        if not x_match:
            return 1
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
    data['BuildingType'] = check_floor_type(
        df.ActualFloor.agg('max')).decode('utf-8')
    return Row(**data)


def buildingHeight(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingCategory(data):
    # print(data, inspect.stack()[0][3])
    return data


def units(data):
    # print(data, inspect.stack()[0][3])
    return data


def unsoldAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and HouseState='未备案'".format(
                         buildingUUID=data['BuildingUUID']))
    data['UnsoldAmount'] = str(df.col.values[0])
    return Row(**data)


def buildingAveragePrice(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPriceRange(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingArea(data):
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['SourceUrl'] = data['SourceUrl']
    return Row(**data)


def extrajson(data):
    # print(data, inspect.stack()[0][3])
    return data
