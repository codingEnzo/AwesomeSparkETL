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

def recordTime():
    data = data.asdict()
    if not data['RecordTime']:
        data['RecordTime'] = nowtime
    return Row(**data)

def projectName():
    data = data.asdict()
    data['RecordTime'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)

def realEstateProjectID():
    data = data.asdict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)

def buildingName():
    data = data.asdict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)

def buildingID():
    return data

def buildingUUID():
    data = data.asdict()
    data['BuildingUUID'] = data['BuildingID']
    return Row(**data)

def unitName():
    return data

def unitID():
    return data

def presalePermitNumber():
    return data

def address():
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjctAddress  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['Address'] = df.col.values()[0]
    return Row(**data)

def onTheGroundFloor():
    data = data.asdict()
    if not data['OnTheGroundFloor']:
        df = pd.read_sql(con=Var.ENGINE,
                         sql="select count(FloorName)  as col from HouseInfoItem where ProjectUUID='{projectUUID}' and FloorName >'0' "\
                         .format(projectUUID=data['ProjectUUID']))
        data['OnTheGroundFloor'] = str(df.col.values()[0])
    return Row(**data)

def theGroundFloor():
    data = data.asdict()
    if not data['TheGroundFloor']:
        df = pd.read_sql(con=Var.ENGINE,
                         sql="select count(FloorName)  as col from HouseInfoItem where ProjectUUID='{projectUUID}' and FloorName <'0' "\
                         .format(projectUUID=data['ProjectUUID']))
        data['TheGroundFloor'] = str(df.col.values()[0])
    return Row(**data)

def estimatedCompletionDate():
    return data

def housingCount():
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(disctinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['HousingCount'] = str(df.col.values()[0])
    return Row(**data)

def floors():
    data = data.asdict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(disctinct(FloorName)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['Floors'] = str(df.col.values()[0])
    return Row(**data)

def elevatorHouse():
    return data

def isHasElevator():
    return data

def elevaltorInfo():
    return data

def buildingStructure():
    data = data.asdict()
    data['BuildingStructure'] = Building.buildingstructure.encode('utf-8').replace('钢混','钢混结构')\
                                                 .replace('框架','框架结构')\
                                                 .replace('钢筋混凝土','钢混结构')\
                                                 .replace('混合','混合结构')\
                                                 .replace('结构结构','结构')\
                                                 .replace('砖混','砖混结构')\
                                                 .replace('框剪','框架剪力墙结构')\
                                                 .replace('钢、','').decode('utf-8')
    return Row(**data)

def buildingType():
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
                     sql="select FloorName as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['BuildingType'] = check_floor_type(
                            df.col.apply(lambda x:int(x) if rule.search(x) else 1).max())
    return Row(**data)

def buildingHeight():
    return data

def buildingCategory():
    return data

def units():
    return data

def unsoldAmount():
    data = data.asdict()
    ["可售","抵押可售","摇号销售","现房销售"]
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseID,HouseState,RecordTime from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    df = df.sort_values(by='RecordTime',ascending=False)\
                            .groupby('HouseID')\
                            .apply(lambda x:x[:1])['HouseState']  
    data['UnsoldAmount'] = df[df.HouseState.isin(["可售","抵押可售","摇号销售","现房销售"])].size.__str__()
    return Row(**data)

def buildingAveragePrice():
    return data

def buildingPriceRange():
    return data

def buildingArea():
    data = data.asdict()
    ["可售","抵押可售","摇号销售","现房销售"]
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseID,MeasuredBuildingArea,RecordTime from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    df = df.sort_values(by='RecordTime',ascending=False)\
                            .groupby('HouseID')\
                            .apply(lambda x:x[:1])['MeasuredBuildingArea']  
    data['UnsoldAmount'] = df.apply(Meth.cleanUnit)\
                             .apply(lambda x:float(x) if x else 0.0)\
                            .sum().__str__()
    return Row(**data)

def remarks():
    return data

def sourceUrl():
    return data

def extraJson():
    return data

