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
from SparkETLCore.Utils import Meth, Var, Config

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
    data['RealEstateProjectID'] = data['ProjectUUID']
    return data


def buildingId(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingID'] = data['BuildingUUID']
    return data


def houseId(data):
    data['HouseID'] = data['HouseID']
    return data


def forecastBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data['ForecastBuildingArea'] = data['ForecastBuildingArea']
    return data


def forecastInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
    return data


def forecastPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data['ForecastPublicArea'] = data['ForecastPublicArea']
    return data


def measuredBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
    return data


def measuredInsideOfBuildingArea(data):
    # print(data, inspect.stack()[0][3])
    data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
    return data


def measuredSharedPublicArea(data):
    # print(data, inspect.stack()[0][3])
    data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
    return data


def isMortgage(data):
    # print(data, inspect.stack()[0][3])
    data['IsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMortgage', '')
    return data


def isAttachment(data):
    # print(data, inspect.stack()[0][3])
    data['IsAttachment'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsAttachment', '')
    return data


def isPrivateUse(data):
    # print(data, inspect.stack()[0][3])
    data['IsPrivateUse'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsPrivateUse', '')
    return data


def isMoveBack(data):
    # print(data, inspect.stack()[0][3])
    data['IsMoveBack'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsMoveBack', '')
    return data


def isSharedPublicMatching(data):
    # print(data, inspect.stack()[0][3])
    data['IsSharedPublicMatching'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraIsSharedPublicMatching', '')
    return data


def buildingStructure(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构')\
        .replace('框架', '框架结构')\
        .replace('钢筋混凝土', '钢混结构')\
        .replace('混合', '混合结构')\
        .replace('结构结构', '结构')\
        .replace('砖混', '砖混结构')\
        .replace('框剪', '框架剪力墙结构')\
        .replace('钢，', '')
    return data


def sellSchedule(data):
    return data


def sellState(data):
    data['SellState'] = data['HouseState']
    return data


def sourceLink(data):
    data['Sourcelink'] = data.get('SourceURL', '')
    return data


def caseTime(data):
    # print(data, inspect.stack()[0][3])
    data['CaseTime'] = str(datetime.datetime.now()) if data[
        'CaseTime'] == '' else data['CaseTime']
    return data


def caseFrom(data):
    data['CaseFrom'] = data.get('SourceURL', '')
    return data


def unitShape(data):
    def translate(x):
        for key in Var.NUMTAB:
            x = x.replace(key, Var.NUMTAB[key])
        return x

    # print(data, inspect.stack()[0][3])
    data['UnitShape'] = translate(data['UnitShape'])
    return data


def unitStructure(data):
    return data


def balconys(data):
    # print(data, inspect.stack()[0][3])
    data['Balconys'] = data['Balconys']
    return data


def unenclosedBalconys(data):
    # print(data, inspect.stack()[0][3])
    data['UnenclosedBalconys'] = data['UnenclosedBalconys']
    return data


def districtName(data):
    # print(data, inspect.stack()[0][3])
    return data


def regionName(data):
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def buildingName(data):
    # print(data, inspect.stack()[0][3])
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data


def presalePermitNumber(data):
    return data


def houseName(data):
    # print(data, inspect.stack()[0][3])
    data['HouseName'] = data['FloorName'] + data['HouseNumber']
    return data


def houseNumber(data):
    # print(data, inspect.stack()[0][3])
    data['HouseNumber'] = data['HouseNumber']
    return data


def totalPrice(data):
    return data


def price(data):
    def calculatePrice(nowInfo, oldInfo=None, houseUseType='Other', measuredArea=0.0):
        totalprice, price = 0, 0
        nowInfo = nowInfo or {}
        oldInfo = oldInfo or {}
        if nowInfo['Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)] not in ['0', '']:
            now_avg_price = float(nowInfo[
                                  'Extra{houseUseType}TotalSoldPrice'.format(houseUseType=houseUseType)])
            now_total_area = float(nowInfo[
                                   'Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)])
            new_total_amount = float(nowInfo[
                'Extra{houseUseType}SoldAmount'.format(houseUseType=houseUseType)])
            old_avg_price = float(oldInfo[
                                  'Extra{houseUseType}TotalSoldPrice'.format(houseUseType=houseUseType)] or 0) if oldInfo else 0
            old_total_area = float(oldInfo[
                                   'Extra{houseUseType}TotalSoldArea'.format(houseUseType=houseUseType)] or 0) if oldInfo else 0
            old_total_amount = float(oldInfo[
                'Extra{houseUseType}SoldAmount'.format(houseUseType=houseUseType)] or 0)
            if new_total_amount - old_total_amount < 1:
                avg_price = now_avg_price
            else:
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
    priceList = data['Price'].split('@#$')

    nowInfo, oldInfo = None, None
    if len(priceList) > 0:
        nowInfo = Meth.jsonLoad(priceList[0]) if priceList[0] else None
    if len(priceList) > 1:
        oldInfo = Meth.jsonLoad(priceList[1]) if priceList[1] else None
    try:
        measuredArea = float(data['ForecastBuildingArea'])
        data['TotalPrice'], data['Price'] = calculatePrice(nowInfo,
                                                           oldInfo,
                                                           houseUseTypeMap.get(
                                                               data['HouseUseType'].strip(), 'Other'),
                                                           measuredArea) if nowInfo else ('0', '0')
    except Exception as e:
        data['TotalPrice'], data['Price'] = '0', '0'
        print(data['ForecastBuildingArea'], e)

    return data


def priceType(data):
    # print(data, inspect.stack()[0][3])
    data['PriceType'] = '成交均价'
    return data


def Address(data):
    return data


def buildingCompletedYear(data):
    return data


def floor(data):
    # print(data, inspect.stack()[0][3])
    data['Floor'] = data['ActualFloor']
    return data


def nominalFloor(data):
    # print(data, inspect.stack()[0][3])
    data['NominalFloor'] = data['FloorName']
    return data


def floors(data):
    # print(data, inspect.stack()[0][3])
    data['Floors'] = data['FloorCount'].replace('层', '')
    return data


def houseUseType(data):
    return data


def dwelling(data):
    return data


def state(data):
    # print(data, inspect.stack()[0][3])
    data['State'] = '明确成交'
    return data


def dealType(data):
    # print(data, inspect.stack()[0][3])
    data['DealType'] = '最新成交'
    return data


def remarks(data):
    return data
