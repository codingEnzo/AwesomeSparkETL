# coding=utf-8
from __future__ import division
import re
import pandas as pd
import datetime
from SparkETLCore.Utils import Var, Meth

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
           'unsoldAmount',
           'projectUUID',
           'unitUUID'
           ]


def recordTime(spark, data):
    nowtime = datetime.datetime.now()
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(spark, data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def unitUUID(spark, data):
    return data


def projectUUID(spark, data):
    data['ProjectUUID'] = data['ProjectUUID']
    return data


def realEstateProjectId(spark, data):
    return data


def buildingName(spark, data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(spark, data):
    return data


def buildingUUID(spark, data):
    return data


def unitName(spark, data):
    return data


def unitId(spark, data):
    return data


def presalePermitNumber(spark, data):
    data['PresalePermitNumber'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresalePermitNumber', ''))
    return data


def address(spark, data):
    sql = u"select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
        projectName=data['ProjectName'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        data['Address'] = df.col.values[-1] if not df.empty else ''
    return data


def onTheGroundFloor(spark, data):
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

    sql = "select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
        buildingUUID=data['BuildingUUID'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        df['ActualFloor'] = df['HouseName'].apply(getFloor)
        ActualFloor = df.ActualFloor.agg('max')
        if ActualFloor != 0:
            data['OnTheGroundFloor'] = ActualFloor
        else:
            data['OnTheGroundFloor'] = None
    return data


def theGroundFloor(spark, data):
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

    sql = u"select distinct HouseName from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
        buildingUUID=data['BuildingUUID'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        df['ActualFloor'] = df['HouseName'].apply(getFloor)
        ActualFloor = df.ActualFloor.agg('max')
        if ActualFloor != 0:
            data['TheGroundFloor'] = ActualFloor
        else:
            data['TheGroundFloor'] = None
    return data


def estimatedCompletionDate(spark, data):
    return data


def housingCount(spark, data):
    sql = u"select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
        buildingUUID=data['BuildingUUID'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        data['TheGroundFloor'] = str(df.col.values[0])
    return data


def floors(spark, data):
    return data


def elevatorHouse(spark, data):
    return data


def isHasElevator(spark, data):
    return data


def elevaltorInfo(spark, data):
    return data


def buildingStructure(spark, data):
    return data


def buildingType(spark, data):
    return data


def buildingHeight(spark, data):
    return data


def buildingCategory(spark, data):
    return data


def units(spark, data):
    return data


def unsoldAmount(spark, data):
    sql = u"select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and HouseState='未备案'".format(
        buildingUUID=data['BuildingUUID'])
    df = spark.sql(sql).toPandas()
    if not df.empty:
        data['UnsoldAmount'] = str(df.col.values[0])
    return data


def buildingAveragePrice(spark, data):
    return data


def buildingPriceRange(spark, data):
    return data


def buildingArea(spark, data):
    return data


def remarks(spark, data):
    return data


def sourceUrl(spark, data):
    return data


def extrajson(spark, data):
    return data
