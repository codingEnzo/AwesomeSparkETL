# coding=utf-8
from __future__ import division
import sys
import datetime
import inspect
import pandas as pd
import numpy as np
import os 
sys.path.append(os.path.dirname(os.getcwd()))
import re
from pyspark.sql import Row
from SparkETLCore.Utils import  Meth, Config,Var
nowtime = datetime.datetime.now() 

METHODS =  ['address',
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
             'elevaltorInfo',
             'elevatorHouse',
             'estimatedCompletionDate',
             'extraJson',
             'floors',
             'housingCount',
             'isHasElevator',
             'onTheGroundFloor',
             'presalePermitNumber',
             'projectName',
             'realEstateProjectID',
             'recordTime',
             'remarks',
             'sourceUrl',
             'theGroundFloor',
             'unitID',
             'unitName',
             'units',
             'unsoldAmount',
             'unitUUID']

def recordTime(data):
    if not data['RecordTime']:
        data['RecordTime'] = nowtime
    return data

def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data

def realEstateProjectID(data):
    return data

def projectUUID(data):
    return data

def unitUUID(data):
    return data

def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data

def buildingID(data):
    return data

def buildingUUID(data):
    return data

def unitName(data):
    return data

def unitID(data):
    return data

def presalePermitNumber(data):
    return data

def address(data):
    if data['ProProjectAddress']:
        data['Address'] =  Meth.cleanName(data['ProProjectAddress'])
    return data

def onTheGroundFloor(data):
    return data

def theGroundFloor(data):
    return data

def estimatedCompletionDate(data):
    if data['EstimatedCompletionDate']:
        data['EstimatedCompletionDate'] = data['EstimatedCompletionDate'].replace('/','-')
    return data

def housingCount(data):
    cache =re.findall(r'(\d+)套',data['ExtraJson'])
    if cache:
        cache = list(map(int,cache))
        data['HousingCount'] = sum(cache).__str__()
    return data

def floors(data):
    if data['HouFloors']:
        data['Floors'] = str(data['HouFloors'])
    return data

def elevatorHouse(data):
    return data

def isHasElevator(data):
    return data

def elevaltorInfo(data):
    return data

def buildingStructure(data):
    if data['BuildingStructure']:
        data['BuildingStructure'] = data['BuildingStructure'].replace('钢混','钢混结构')\
                                                     .replace('框架','框架结构')\
                                                     .replace('钢筋混凝土','钢混结构')\
                                                     .replace('混合','混合结构')\
                                                     .replace('结构结构','结构')\
                                                     .replace('砖混','砖混结构')\
                                                     .replace('框剪','框架剪力墙结构')\
                                                     .replace('钢、','')
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
    if data['BuildingArea']:
        data['BuildingArea'] = Meth.cleanUnit(data['BuildingArea']) 
    return data

def remarks(data):
    return data

def sourceUrl(data):
    return data

def extraJson(data):
    return data

