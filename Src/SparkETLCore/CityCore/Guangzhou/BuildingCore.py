# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import re
import sys
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['address', 'buildingArea', 'buildingAveragePrice', 'buildingCategory', 'buildingHeight', 'buildingId', 'buildingName', 'buildingPriceRange', 'buildingStructure', 'buildingType', 'buildingUUID', 'elevaltorInfo', 'elevatorHouse', 'estimatedCompletionDate', 'extrajson', 'floors',
           'housingCount', 'isHasElevator', 'onTheGroundFloor', 'presalePermitNumber', 'projectName', 'realEstateProjectId', 'recordTime', 'remarks', 'sourceUrl', 'theGroundFloor',
           'unitId', 'unitName', 'units', 'unsoldAmount']


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
    data['BuildingID'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraBuildingID', ''))
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
    data['PresalePermitNumber'] = Meth.jsonDumps(
        data['PresalePermitNumber'].split('@#$'))
    return data


def address(data):
    # print(data, inspect.stack()[0][3])
    return data


def onTheGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    data['OnTheGroundFloor'] = str(0)
    return data


def theGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    data['TheGroundFloor'] = str(0)
    return data


def estimatedCompletionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
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
    data['BuildingStructure'] = Meth.jsonDumps(
        data['BuildingStructure'].split('@#$'))
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
        elif floorname >= 33:
            return '超高层(33)'
        else:
            return ''

    def getFloor(x):
        x_match = re.search(r'(\d+)', x)
        try:
            if x_match:
                res = int(x_match.group(1))
            else:
                res = 1
        except Exception as e:
            res = 1
        return res
    # print(data, inspect.stack()[0][3])
    data['BuildingType'] = ''
    return data


def buildingHeight(data):
    def getFloorHeight(x):
        x_match = re.search(r'(\d+[\.]\d+)', x)
        try:
            if x_match:
                res = float(x_match.group(1))
            else:
                res = 0.0
        except Exception as e:
            res = 0.0
        return res

    # print(data, inspect.stack()[0][3])
    data['BuildingHeight'] = str(0)
    return data


def buildingCategory(data):
    # print(data, inspect.stack()[0][3])
    return data


def units(data):
    # print(data, inspect.stack()[0][3])
    return data


def unsoldAmount(data):
    # print(data, inspect.stack()[0][3])
    data['UnsoldAmount'] = str(data['UnsoldAmount'])
    return data


def buildingAveragePrice(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPriceRange(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingArea(data):
    def getMeasuredBuildingArea(x):
        x_match = re.search(r'(\d+[\.]\d+)', x)
        try:
            if x_match:
                res = float(x_match.group(1))
            else:
                res = 0.0
        except Exception as e:
            res = 0.0
        return res

    # print(data, inspect.stack()[0][3])
    data['BuildingArea'] = str(0)
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    data['SourceUrl'] = 'http://www.gzcc.gov.cn/housing/search/project/sellForm.jsp?pjID={0}'.\
        format(str(Meth.jsonLoad(data['ExtraJson']).get(
            'ExtraBuildingID', '')))
    return data


def extrajson(data):
    # print(data, inspect.stack()[0][3])
    return data
