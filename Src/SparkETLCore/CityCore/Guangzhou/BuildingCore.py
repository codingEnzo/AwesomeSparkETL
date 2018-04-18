# coding=utf-8
from __future__ import division
import re
import sys
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

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
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingID'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraBuildingID', ''))
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select PresalePermitNumber as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['PresalePermitNumber'] = str(df.col.values[-1]) if not df.empty else ''
    return Row(**data)


def address(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress as col from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime".format(
                         projectName=data['ProjectName']))
    data['Address'] = df.col.values[-1] if not df.empty else ''
    # data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
    return Row(**data)


def onTheGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and ActualFloor >= '1'".format(
                         buildingUUID=data['BuildingUUID']))
    data['OnTheGroundFloor'] = str(df.col.values[-1])
    return Row(**data)


def theGroundFloor(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and ActualFloor < '1'".format(
                         buildingUUID=data['BuildingUUID']))
    data['TheGroundFloor'] = str(df.col.values[-1])
    return Row(**data)


def estimatedCompletionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['TheGroundFloor'] = str(df.col.values[0])
    return Row(**data)


def floors(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['Floors'] = str(df.col.values[0])
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct BuildingStructure as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['BuildingStructure'] = Meth.jsonDumps(
        list(set(df.col.values) - set([''])))
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct ActualFloor from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['ActualFloor'] = df['ActualFloor'].apply(getFloor)
    data['BuildingType'] = check_floor_type(df.ActualFloor.agg('max')).decode('utf-8')
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct FloorHight from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['FloorHight'] = df['FloorHight'].apply(getFloorHeight)
    # print(df.FloorHight.values)
    data['BuildingHeight'] = str(df.FloorHight.agg('sum'))
    return Row(**data)


def buildingCategory(data):
    # print(data, inspect.stack()[0][3])
    return data


def units(data):
    # print(data, inspect.stack()[0][3])
    return data


def unsoldAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnsoldAmount'] = str(data['UnsoldAmount'])
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct MeasuredBuildingArea from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    df['MeasuredBuildingArea'] = df['MeasuredBuildingArea'].apply(getMeasuredBuildingArea)
    data['BuildingArea'] = str(df.MeasuredBuildingArea.agg('sum'))
    return Row(**data)


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['SourceUrl'] = 'http://www.gzcc.gov.cn/housing/search/project/sellForm.jsp?pjID={0}'.\
        format(str(Meth.jsonLoad(data['ExtraJson']).get(
            'ExtraBuildingID', '')))
    return Row(**data)


def extrajson(data):
    # print(data, inspect.stack()[0][3])
    return data
