# coding=utf-8
from __future__ import division

import re
import sys
import pandas as pd
import numpy as np



from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['address',
           'buildingArea',
           'buildingAveragePrice',
           'buildingCategory',
           'buildingHeight',
           'buildingID',
           'buildingName',
           'buildingPriceRange',
           'buildingStructure',
           'buildingType',
           'buildingUUID',
           'estimatedCompletionDate',
           'extrajson',
           'floors',
           'housingCount',
           'onTheGroundFloor',
           'presalePermitNumber',
           'projectName',
           'realEstateProjectID',
           'recordtime',
           'remarks',
           'sourceUrl',
           'theGroundFloor',
           'unitID',
           'unitName',
           'units',
           'unsoldAmount']


def recordtime(data):
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectID(data):
    return data


def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def buildingID(data):
    return data


def buildingUUID(data):
    return data


def unitName(data):
    data['UnitName'] = Meth.cleanName(data['UnitName'])
    return data


def unitID(data):
    return data


def presalePermitNumber(data):
    arr = data['PresalePermitNumber'].replace('，'.decode('utf-8'), ',').split(',')
    data['PresalePermitNumber'] = Meth.jsonDumps(filter(lambda x: x and x.strip(), arr))
    return data


def address(data):
    data['Address'] = Meth.cleanName(data['Address'])
    return data


def onTheGroundFloor(data):
    arr = data['FloorName'].split('@#$')
    count = 0
    for i in arr:
        c = re.search('-?\d+', i)
        if c and int(c.group()) > 0:
            count += 1
    data['OnTheGroundFloor'] = str(count)
    return data


def theGroundFloor(data):
    arr = data['FloorName'].split('@#$')
    count = 0
    for i in arr:
        c = re.search('-?\d+', i)
        if c and int(c.group()) < 0:
            count += 1
    data['TheGroundFloor'] = str(count)
    return data


def estimatedCompletionDate(data):
    return data


def housingCount(data):
    data['HousingCount'] = str(data['HousingCount'])
    return data


def floors(data):
    arr = data['FloorName'].split('@#$')
    count = 0
    for i in arr:
        c = re.search('-?\d+', i)
        if c:
            count += 1
    data['Floors'] = str(count)
    return data


def buildingStructure(data):
    return data


def buildingType(data):
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
    return data


def remarks(data):
    return data


def sourceUrl(data):
    if not data['SourceUrl']:
        data['SourceUrl'] = Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingURL', '')
    return data


def extrajson(data):
    return data
