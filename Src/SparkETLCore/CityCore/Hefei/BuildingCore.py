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
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
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
    data = data.asDict()
    if data['EstimatedCompletionDate']:
        data['EstimatedCompletionDate'] = data['EstimatedCompletionDate'].replace('/','-')
    return Row(**data)

def housingCount(data):
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
    #                  .format(projectUUID=data['ProjectUUID']))
    # data['HousingCount'] = str(df.col.values[0])
    # return Row(**data)
    return data

def floors(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct(FloorName)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))

    data['Floors'] = str(df.col.values[0])
    return Row(**data)

def elevatorHouse(data):
    return data

def isHasElevator(data):
    return data

def elevaltorInfo(data):
    return data

def buildingStructure(data):
    data = data.asDict()
    data['BuildingStructure'] = data['BuildingStructure'].encode('utf-8').replace('钢混','钢混结构')\
                                                 .replace('框架','框架结构')\
                                                 .replace('钢筋混凝土','钢混结构')\
                                                 .replace('混合','混合结构')\
                                                 .replace('结构结构','结构')\
                                                 .replace('砖混','砖混结构')\
                                                 .replace('框剪','框架剪力墙结构')\
                                                 .replace('钢、','').decode('utf-8')
    return Row(**data)

def buildingType(data):
    # rule = re.compile('\-?\d+')
    # def check_floor_type(floorname):
    #     if floorname <= 3:
    #         return '低层(1-3)'
    #     elif floorname <= 6:
    #         return '多层(4-6)'
    #     elif floorname <= 11:
    #         return '小高层(7-11)'
    #     elif floorname <= 18:
    #         return '中高层(12-18)'
    #     elif floorname <= 32:
    #         return '高层(19-32)'
    #     elif floorname >= 33:
    #         return '超高层(33)'
    #     else:
    #         return ''
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select FloorName as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
    #                  .format(projectUUID=data['ProjectUUID']))
    # if not df.empty:
    #     data['BuildingType'] = check_floor_type(
    #                             df.col.apply(lambda x:int(x) if rule.search(x) else 1).max()).decode('utf-8')
    # return Row(**data)
    return data

def buildingHeight(data):
    return data

def buildingCategory(data):
    return data

def units(data):
    return data

def unsoldAmount(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseID,HouseState,RecordTime from HouseInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    if not df.empty:
        df = df.sort_values(by='RecordTime',ascending=False)\
                                .groupby('HouseID')\
                                .apply(lambda x:x[:1])  
        data['UnsoldAmount'] = df.HouseID[df.HouseState.isin(["可售","抵押可售","摇号销售","现房销售"])].size.__str__()
    return Row(**data)

def buildingAveragePrice(data):
    return data

def buildingPriceRange(data):
    return data

def buildingArea(data):
    data = data.asDict()
    data['BuildingArea'] = Meth.cleanUnit(data['BuildingArea']) 
    return Row(**data)

def remarks(data):
    return data

def sourceUrl(data):
    return data

def extraJson(data):
    return data

