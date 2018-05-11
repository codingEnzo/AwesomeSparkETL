# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime
import inspect
import sys

import demjson
import pandas as pd

from Utils import Meth

sys.path.append('/home/lin/Dev/AwesomeSparkETL/Src/SparkETLCore')

ENGINE = Meth.getEngine("spark_test")

METHODS = [
    'address', 'buildingArea', 'buildingAveragePrice', 'buildingCategory',
    'buildingHeight', 'buildingId', 'buildingName', 'buildingPriceRange',
    'buildingStructure', 'buildingType', 'buildingUUID', 'elevaltorInfo',
    'elevatorHouse', 'estimatedCompletionDate', 'extraJson', 'floors',
    'housingCount', 'isHasElevator', 'onTheGroundFloor', 'presalePermitNumber',
    'projectName', 'realestateProjectId', 'recordTime', 'remarks', 'sourceUrl',
    'theGroundFloor', 'unitId', 'unitName', 'units', 'unsoldAmount'
]


def recordTime(data):
    print(inspect.stack()[0][3])
    if not data.get("RecordTime"):
        nt = datetime.datetime.now()
        data['RecordTime'] = nt
    return data


def projectName(data):
    return data


def realestateProjectId(data):
    return data


def buildingName(data):
    return data


def buildingId(data):
    extraj = data.get("ExtraJson")
    if extraj:
        data['BuildingId'] = demjson.decode(extraj).get("ExtraBuildingID", "")
    return data


def buildingUUID(data):
    return data


def unitName(data):
    b_uuid = data['BuildingUUID']
    sql = "SELECT HouseInfoItem.UnitName FROM HouseInfoItem " \
        "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _ = list(set(query['UnitName']))
        data['UnitName'] = demjson.encode(_)

    return data


def unitId(data):
    return data


def presalePermitNumber(data):
    return data


def address(data):
    return data


def onTheGroundFloor(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.MeasuredBuildingArea, HouseInfoItem.HouseName FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     query['Floor'] = query.apply(
    #         lambda x: Meth.getFloor(x['HouseName']), axis=1)
    #     _ = query['MeasuredBuildingArea'][query['Floor'] > 0].count()
    #     data['OnTheGroundFloor'] = str(_)

    return data


def theGroundFloor(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.MeasuredBuildingArea, HouseInfoItem.HouseName FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     query['Floor'] = query.apply(
    #         lambda x: Meth.getFloor(x['HouseName']), axis=1)
    #     _ = query['MeasuredBuildingArea'][(query['Floor'] < 1) & (query['Floor'] != 0)] \
    #         .count()
    #     data['OnTheGroundFloor'] = str(_)

    return data


def estimatedCompletionDate(data):
    return data


def housingCount(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     data['HousingCount'] = str(query['MeasuredBuildingArea'].count())
    return data


def floors(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.HouseName FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     try:
    #         query['Floor'] = query.apply(
    #             lambda x: int(Meth.getFloor(x['HouseName'])), axis=1)
    #         data['Floor'] = str(query['Floor'].count())
    #     except ValueError:
    #         print('BuildingUUID: ', b_uuid)

    return data


def elevatorHouse(data):
    return data


def isHasElevator(data):
    return data


def elevaltorInfo(data):
    return data


def buildingStructure(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.BuildingStructure FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     _ = list(
    #         set(query['BuildingStructure'][query['BuildingStructure'] != ""]))
    #     _ = list(map(lambda i: Meth.cleanBuildingStructure(i), _))
    #     data['BuildingStructure'] = demjson.encode(_)

    return data


def buildingType(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.HouseName FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     query['Floor'] = query.apply(
    #         lambda x: Meth.getFloor(x['HouseName']), axis=1)
    #     _ = Meth.bisectCheckFloorType(query['Floor'].max())
    #     data['BuildingType'] = _

    return data


def buildingHeight(data):
    return data


def buildingCategory(data):
    return data


def units(data):
    return data


def unsoldAmount(data):
    return data


def buildingAveragePrice(data):
    return data


def buildingPriceRange(data):
    return data


def buildingArea(data):
    # b_uuid = data['BuildingUUID']
    # sql = "SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem " \
    #     "WHERE HouseInfoItem.BuildingUUID = '{}'".format(b_uuid)
    # query = pd.read_sql(sql, ENGINE)
    # if not query.empty:
    #     query["MeasuredBuildingArea"] = query.apply(
    #         lambda x: float(x['MeasuredBuildingArea']) if x['MeasuredBuildingArea'] else 0.0,
    #         axis=1)
    #     data['BuildingArea'] = str(query['MeasuredBuildingArea'].sum())

    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJson(data):
    return data
