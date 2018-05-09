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
reload(sys)
sys.setdefaultencoding('utf8')

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
             'unsoldAmount']

def recordTime(data):
    data = data.asDict()
    if not data['RecordTime']:
        data['RecordTime'] = nowtime
    return Row(**data)

def projectName(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select (ProjectName)  as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress!='' "\
                     .format(projectUUID=data['ProjectUUID']))
    data['ProjectName'] = Meth.cleanName(df.col.values[0])
    return Row(**data)

def realEstateProjectID(data):
    return data

def projectUUID(data):
    return data

def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)

def buildingID(data):
    return data

def buildingUUID(data):
    return data

def unitName(data):
    return data

def unitID(data):
    return data

def presalePermitNumber(data):
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select (ProjectName)  as col from ProjectInfoItem where PresalePermitTie='{PresellUUID}' and ProjectAddress!='' "\
                     .format(projectUUID=data['PresellUUID'])).fillna('')
    data['PresalePermitNumber'] = Meth.cleanName(df.col.values[0])
    return data

def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select (ProjectAddress)  as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress!='' "\
                     .format(projectUUID=data['ProjectUUID']))
    data['Address'] =  Meth.cleanName(df.col.values[0])
    return Row(**data)

def onTheGroundFloor(data):
    # data = data.asDict()
    # if not data['OnTheGroundFloor']:
    #     df = pd.read_sql(con=Var.ENGINE,
    #                      sql="select count(FloorName)  as col from HouseInfoItem where ProjectUUID='{projectUUID}' and FloorName >'0' "\
    #                      .format(projectUUID=data['ProjectUUID']))
    #     data['OnTheGroundFloor'] = str(df.col.values[0])
    # return Row(**data)
    return data

def theGroundFloor(data):
    # data = data.asDict()
    # if not data['TheGroundFloor']:
    #     df = pd.read_sql(con=Var.ENGINE,
    #                      sql="select count(FloorName)  as col from HouseInfoItem where ProjectUUID='{projectUUID}' and FloorName <'0' "\
    #                      .format(projectUUID=data['ProjectUUID']))
    #     data['TheGroundFloor'] = str(df.col.values[0])
    # return Row(**data)
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
    return data

def extraJson(data):
    return data

