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
             'unsoldAmount']

def recordTime(data):
    data = data.asdict()
    if not data['RecordTime']:
        data['RecordTime'] = nowtime
    return Row(**data)

def projectName(data):
    data = data.asdict()
    data['RecordTime'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)

def realEstateProjectID(data):
    data = data.asdict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)

def buildingName(data):
    data = data.asdict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)

def buildingID(data):
    return data

def buildingUUID(data):
    data = data.asdict()
    data['BuildingUUID'] = data['BuildingID']
    return Row(**data)

def unitName(data):
    return data

def unitID(data):
    return data

def presalePermitNumber(data):
    data = data.asdict()
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return Row(**data)

def address(data):
    data = data.asdict()
    data['address'] = Meth.cleanName(data['address'])
    return Row(**data)

def onTheGroundFloor(data):
    data = data.asdict()
    if not data['OnTheGroundFloor']:
        df = pd.read_sql(con=Var.ENGINE,
                         sql="select count(FloorName)  as col from HouseInfoItem where BuildingUUID='{BuildingUUID}' and FloorName >'0' "\
                         .format(BuildingUUID=data['BuildingUUID']))
        data['OnTheGroundFloor'] = str(df.col.values()[0])
    return Row(**data)

def theGroundFloor(data):
    data = data.asdict()
    if not data['TheGroundFloor']:
        df = pd.read_sql(con=Var.ENGINE,
                         sql="select count(FloorName)  as col from HouseInfoItem where BuildingUUID='{BuildingUUID}' and FloorName <'0' "\
                         .format(BuildingUUID=data['BuildingUUID']))
        data['TheGroundFloor'] = str(df.col.values()[0])
    return Row(**data)

def estimatedCompletionDate(data):
    return data

def housingCount(data):
    return data

def floors(data):
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(disctinct(FloorName)) as col from HouseInfoItem where BuildingUUID='{BuildingUUID}'"\
                     .format(BuildingUUID=data['BuildingUUID']))
    data['Floors'] = str(df.col.values()[0])
    return Row(**data)

def elevatorHouse(data):
    return data

def isHasElevator(data):
    return data

def elevaltorInfo(data):
    return data

def buildingStructure(data):
    return data    data['PresaleBuildingAmount'] = str(df.col.values[0])

def buildingType(data):
    rule = re.compile('\-?\d+')
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
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select FloorName as col from HouseInfoItem where BuildingUUID='{BuildingUUID}'"\
                     .format(BuildingUUID=data['BuildingUUID']))
    data['BuildingType'] = check_floor_type(
                            df.col.apply(lambda x:int(x) if rule.search(x) else 1).max())
    return Row(**data)

def buildingHeight(data):
    return data

def buildingCategory(data):
    return data

def units(data):
    return data

def unsoldAmount(data):
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseID,HouseState,RecordTime from HouseInfoItem where BuildingUUID='{BuildingUUID}'"\
                     .format(BuildingUUID=data['BuildingUUID']))

    df = df.sort_values(by='RecordTime',ascending=False)\
                            .groupby('HouseID')\
                            .apply(lambda x:x[:1])['HouseState']  
    data['UnsoldAmount'] = df[df.HouseState=="可售"].size.__str__()
    return Row(**data)

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

