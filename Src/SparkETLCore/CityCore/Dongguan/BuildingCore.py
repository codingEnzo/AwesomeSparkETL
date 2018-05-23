# coding=utf-8
from __future__ import division
import datetime
from SparkETLCore.Utils import Meth

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
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(data):
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    return data


def buildingId(data):
    return data


def buildingUUID(data):
    return data


def unitName(data):
    return data


def unitId(data):
    return data


def presalePermitNumber(data):
    return data


def address(data):
    return data


def onTheGroundFloor(data):
    return data


def theGroundFloor(data):
    return data


def estimatedCompletionDate(data):
    return data


def housingCount(data):
    return data


def floors(data):
    return data


def elevatorHouse(data):
    return data


def isHasElevator(data):
    return data


def elevaltorInfo(data):
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
    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceUrl', ''))
    return data


def extrajson(data):
    return data
