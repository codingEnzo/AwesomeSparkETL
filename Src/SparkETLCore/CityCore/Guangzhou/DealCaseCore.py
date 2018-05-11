# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import sys
import datetime
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Meth, Var, Config

METHODS = ['Address',
           'balconys',
           'buildingCompletedYear',
           'buildingId',
           'buildingName',
           'buildingStructure',
           'caseFrom',
           'caseTime',
           'dealType',
           'districtName',
           'dwelling',
           'floor',
           'floors',
           'forecastBuildingArea',
           'forecastInsideOfBuildingArea',
           'forecastPublicArea',
           'houseId',
           'houseName',
           'houseNumber',
           'houseUseType',
           'isAttachment',
           'isMortgage',
           'isMoveBack',
           'isPrivateUse',
           'isSharedPublicMatching',
           'measuredBuildingArea',
           'measuredInsideOfBuildingArea',
           'measuredSharedPublicArea',
           'nominalFloor',
           'presalePermitNumber',
           'price',
           'priceType',
           'projectName',
           'realEstateProjectId',
           'regionName',
           'remarks',
           'sellSchedule',
           'sellState',
           'sourceLink',
           'state',
           'totalPrice',
           'unenclosedBalconys',
           'unitShape',
           'unitStructure']


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingID'] = data['BuildingUUID']
    return Row(**data)


def houseId(data):
    data = data.asDict()
    data['HouseID'] = data['HouseID']
    return Row(**data)


def forecastBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastBuildingArea'] = data['ForecastBuildingArea']
    return Row(**data)


def forecastInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
    return Row(**data)


def forecastPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ForecastPublicArea'] = data['ForecastPublicArea']
    return Row(**data)


def measuredBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
    return Row(**data)


def measuredInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
    return Row(**data)


def measuredSharedPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
    return Row(**data)


def isMortgage(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMortgage', '')
    return Row(**data)


def isAttachment(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsAttachment'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsAttachment', '')
    return Row(**data)


def isPrivateUse(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsPrivateUse'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsPrivateUse', '')
    return Row(**data)


def isMoveBack(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsMoveBack'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMoveBack', '')
    return Row(**data)


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['IsSharedPublicMatching'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsSharedPublicMatching', '')
    return Row(**data)


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构')\
        .replace('框架', '框架结构')\
        .replace('钢筋混凝土', '钢混结构')\
        .replace('混合', '混合结构')\
        .replace('结构结构', '结构')\
        .replace('砖混', '砖混结构')\
        .replace('框剪', '框架剪力墙结构')\
        .replace('钢，', '')
    return Row(**data)


def sellSchedule(data):
    return data


def sellState(data):
    return data


def sourceLink(data):
    data = data.asDict()
    data['Sourcelink'] = data.get('SourceURL', '')
    return Row(**data)


def caseTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['CaseTime'] = str(datetime.datetime.now()) if data[
        'CaseTime'] == '' else data['CaseTime']
    return Row(**data)


def caseFrom(data):
    data = data.asDict()
    data['CaseFrom'] = data.get('SourceURL', '')
    return Row(**data)


def unitShape(data):
    def translate(x):
        for key in Var.NUMTAB:
            x = x.replace(key, Var.NUMTAB[key])
        return x

    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnitShape'] = translate(data['UnitShape'])
    return Row(**data)


def unitStructure(data):
    return data


def balconys(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Balconys'] = data['Balconys']
    return Row(**data)


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return Row(**data)


def districtName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select DistrictName as Dcol, RegionName as Rcol, ProjectAddress as Acol, PresalePermitNumber as Pcol from ProjectInfoItem where ProjectName='{projectName}' order by RecordTime DESC limit 1".format(
                         projectName=data['ProjectName']))
    data['DistrictName'] = df.Dcol.values[-1] if not df.empty else ''
    data['RegionName'] = df.Rcol.values[-1] if not df.empty else ''
    data['Address'] = df.Acol.values[-1] if not df.empty else ''
    data['PresalePermitNumber'] = df.Pcol.values[-1] if not df.empty else ''
    return Row(**data)


def regionName(data):
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def presalePermitNumber(data):
    return data


def houseName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['HouseName'] = data['FloorName'] + data['HouseNumber']
    return Row(**data)


def houseNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['HouseNumber'] = data['HouseNumber']
    return Row(**data)


def totalPrice(data):
    return data


def price(data):
    def calculatePrice(nowInfo, oldInfo=None, houseUseType='Other', measuredArea=0.0):
        totalprice, price = 0, 0
        if nowInfo['ExtraJson']['Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)] not in ['0', '']:
            now_avg_price = float(nowInfo['ExtraJson'][
                                  'Extra{houseUseType}TotalSoldPrice'.format(houseUseType=houseUseType)])
            now_total_area = float(nowInfo['ExtraJson'][
                                   'Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)])
            old_avg_price = float(oldInfo['ExtraJson'][
                                  'Extra{houseUseType}TotalSoldPrice'.format(houseUseType=houseUseType)] or 0) if oldInfo else 0
            old_total_area = float(oldInfo['ExtraJson'][
                                   'Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)] or 0) if oldInfo else 0
            if (now_total_area != old_total_area) or (now_avg_price != old_avg_price):
                now_total_value = now_avg_price * now_total_area
                old_total_value = old_avg_price * old_total_area
                avg_price = (now_total_value - old_total_value) / (
                    now_total_area - old_total_area)
                if avg_price < 0:
                    avg_price = now_avg_price
            else:
                avg_price = now_avg_price
            totalprice = round((avg_price * measuredArea), 3)
            price = round(avg_price, 3)
        return totalprice, price

    # print(data, inspect.stack()[0][3])
    houseUseTypeMap = {'住宅': 'Housing',
                       '车位': 'Parking',
                       '商业': 'Shop',
                       '办公': 'Office',
                       '其他': 'Other'}
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql='SELECT * FROM ProjectInfoItem WHERE ProjectUUID="{0}" AND RecordTime <="{1}" ORDER BY RecordTime LIMIT 2'.
                     format(data['ProjectUUID'], str(data['CaseTime'])))
    df['ExtraJson'] = df['ExtraJson'].apply(Meth.jsonLoad)
    infoList = [row.to_dict() for index, row in df.iterrows()]
    if len(infoList) > 1:
        nowInfo, oldInfo = infoList[0], infoList[1]
    else:
        nowInfo, oldInfo = infoList[0], None
    data['totalPrice'], data['price'] = calculatePrice(nowInfo,
                                                       oldInfo,
                                                       houseUseTypeMap.get(
                                                           data['HouseUseType'].strip(), 'Other'),
                                                       float(data['ForecastBuildingArea']))
    return Row(**data)


def priceType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PriceType'] = '成交均价'
    return Row(**data)


def Address(data):
    return data


def buildingCompletedYear(data):
    return data


def floor(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Floor'] = data['ActualFloor']
    return Row(**data)


def nominalFloor(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['NominalFloor'] = data['FloorName']
    return Row(**data)


def floors(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Floors'] = data['FloorCount'].replace('层', '')
    return Row(**data)


def houseUseType(data):
    return data


def dwelling(data):
    return data


def state(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['State'] = '明确成交'
    return Row(**data)


def dealType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['DealType'] = '最新成交'
    return Row(**data)


def remarks(data):
    return data
