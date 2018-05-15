# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import re
import pandas as pd
import numpy as np
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
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])

    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])

    data['RealEstateProjectID'] = data['ProjectUUID']
    return data


def buildingName(data):
    # print(data, inspect.stack()[0][3])

    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingUUID(data):
    # print(data, inspect.stack()[0][3])
    return data


def unitName(data):
    # print(data, inspect.stack()[0][3])
    return data


def unitId(data):
    # print(data, inspect.stack()[0][3])
    return data


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    return data


def address(data):
    data['Address'] = Meth.cleanName(data['Address'])
    return data


def onTheGroundFloor(data):
    # print(data, inspect.stack()[0][3])

    arr = data['ActualFloor'].split('@#$')
    if '' in arr:
        arr.remove('')
    arr = list(filter(lambda x: int(x) > 0, arr))
    data['OnTheGroundFloor'] = str(len(arr))
    return data


def theGroundFloor(data):
    arr = data['ActualFloor'].split('@#$')
    if '' in arr:
        arr.remove('')
    arr = list(filter(lambda x: int(x) < 0, arr))
    data['TheGroundFloor'] = str(len(arr))
    return data


def estimatedCompletionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    return data


def floors(data):
    arr = data['ActualFloor'].split('@#$')
    arr = list(filter(lambda x: (x != 'null' and x != ''), arr))
    data['Floors'] = str(len(arr))
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

    arr = data['BuildingStructure'].split('@#$')
    arr = list(filter(lambda x: (x != 'null' and x != ''), arr))
    data['BuildingStructure'] = Meth.jsonDumps(arr)
    return data


def buildingType(data):
    return data


def buildingHeight(data):
    return data


def buildingCategory(data):
    # print(data, inspect.stack()[0][3])
    return data


def units(data):
    # print(data, inspect.stack()[0][3])
    return data


def unsoldAmount(data):
    return data


def buildingAveragePrice(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPriceRange(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingArea(data):
    def getMeasuredBuildingArea(arr):
        x_match = re.search(r'(\d+[\.]\d+)', x)
        try:
            if x_match:
                res = float(x_match.group(1))
            else:
                res = 0.0
        except Exception as e:
            res = 0.0
        return res

    bArea = 0.0
    for area in data['MeasuredBuildingArea'].split('@#$'):
        c = re.search('\d+[\.]\d+', area)
        bArea += float(c.group()) if c else 0.0
    data['BuildingArea'] = '' if bArea == 0 else str(round(bArea, 2))
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    return data


def extrajson(data):
    # print(data, inspect.stack()[0][3])
    return data
