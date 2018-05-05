# coding=utf-8
from __future__ import division
import re
import sys
import inspect
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

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
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return data


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingID'] = data['BuildingID']
    return data


def buildingUUID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingUUID'] = data['BuildingUUID']
    return data


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
    return data


def address(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    return data


def onTheGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    def getFloor(x):
        if x == '':
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
                     sql=u"select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['HouseName'].apply(getFloor)
    ActualFloor = df.ActualFloor.agg('max')
    if ActualFloor != 0:
        data['OnTheGroundFloor'] = ActualFloor
    else:
        data['OnTheGroundFloor'] = None
    return data


def theGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    def getFloor(x):
        if x == '':
            return 0
        if x[0] == '-':
            x_match = re.search(r'(\d+)', x)
            if not x_match:
                return 0
            if len(x_match.group(1)) <= 3:
                res = int(x_match.group(1)[0])
            else:
                res = int(x_match.group(1)[0:2])
        else:
            res = 0
        return res
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['HouseName'].apply(getFloor)
    ActualFloor = df.ActualFloor.agg('max')
    if ActualFloor != 0:
        data['TheGroundFloor'] = ActualFloor
    else:
        data['TheGroundFloor'] = None
    return data


def estimatedCompletionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['TheGroundFloor'] = str(df.col.values[0])
    return data


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
    # print(data, inspect.stack()[0][3])
    return data


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
                     sql=u"select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and HouseState='未备案'".format(
                         buildingUUID=data['BuildingUUID']))
    data['UnsoldAmount'] = str(df.col.values[0])
    return data


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
    return data


def extrajson(data):
    # print(data, inspect.stack()[0][3])
    return data
