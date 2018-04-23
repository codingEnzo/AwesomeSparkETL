# coding=utf-8
from __future__ import division
import sys
import inspect
import pandas as pd
import numpy as np
import os 
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from SparkETLCore.Utils import  Meth, Config,Var


METHODS = ['actualFloor',
     'address',
     'balconys',
     'buildingId',
     'buildingName',
     'buildingStructure',
     'caseTime',
     'city',
     'decoration',
     'decorationPrice',
     'districtname',
     'dwelling',
     'extraJSON',
     'floorCount',
     'floorName',
     'floorRight',
     'floorType',
     'forecastBuildingArea',
     'forecastInsideOfBuildingArea',
     'forecastPublicArea',
     'halls',
     'houseId',
     'houseLabel',
     'houseLabelLatest',
     'houseName',
     'houseNature',
     'houseNumber',
     'houseSalePrice',
     'houseShape',
     'houseState',
     'houseStateLatest',
     'houseType',
     'houseUUID',
     'houseUseType',
     'isAttachment',
     'isMortgage',
     'isMoveback',
     'isPrivateUse',
     'isSharedPublicMatching',
     'kitchens',
     'measuredBuildingArea',
     'measuredInsideOfBuildingArea',
     'measuredSharedPublicArea',
     'measuredUndergroundArea',
     'natureOfPropertyRight',
     'price',
     'priceType',
     'projectName',
     'realEstateProjectId',
     'remarks',
     'rooms',
     'salePriceByBuildingArea',
     'salePriceByInsideOfBuildingArea',
     'sellSchedule',
     'sellState',
     'sourceUrl',
     'toilets',
     'totalPrice',
     'toward',
     'unEnclosedBalconys',
     'unitId',
     'unitName',
     'unitShape',
     'unitStructure']


def caseTime(data):
    return data


def projectName(data):
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectId(data):
    return data


def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingId(data):
    data = data.asDict()
    data['BuildingID'] = Meth.cleanName(data['BuildingUUID'])
    return Row(**data)


def city(data):
    data = data.asDict()
    data['City'] = '合肥'.decode('utf-8')
    return Row(**data)


def districtname(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select districtname  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['DistrictName'] = Meth.cleanName(df.col.values[0]).decode('utf-8')

def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    data = data.asDict()
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return Row(**data)

def houseName(data):
    data = data.asDict()
    data['HouseName'] =  data['FloorName'].decode('utf-8')\
                        +u'层'\
                        +Meth.cleanName(data['HouseNumber']).decode('utf-8')
    return Row(**data)


def houseId(data):
    return data


def houseUUID(data):
    data = data.asDict()
    data['HouseUUID'] = data['HouseID']
    return Row(**data)


def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['Address'] = Meth.cleanName(df.col.values[0]).decode('utf-8')

def floorName(data):
    data = data.asDict()
    data['FloorName'] =  data['FloorName'].decode('utf-8')+u'层'
    return Row(**data)

def actualFloor(data):
    data = data.asDict()
    data['ActualFloor'] =  data['FloorName'].decode('utf-8')
    return Row(**data)

def floorCount(data):
    return data


def floorType(data):
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
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    if rule.search(data['FloorName']):
        floor = int(rule.search(data['FloorName']).group())
    else:
        floor =1
    data['FloorType'] = check_floor_type(floor).decode('utf-8')
    return Row(**data)


def floorRight(data):
    return data


def unitShape(data):
    data = data.asDict()
    data['UnitShape'] = Meth.jsonLoad(data['ExtraJson']).get('ExtraHouseType','').decode('utf-8')
    return Row(**data)


def unitStructure(data):
    return data


def rooms(data):
    return data


def halls(data):
    return data


def kitchens(data):
    return data


def toilets(data):
    return data


def balconys(data):
    return data


def unEnclosedBalconys(data):
    return data


def houseShape(data):
    return data


def dwelling(data):
    return data


def forecastBuildingArea(data):
    return data


def forecastInsideOfBuildingArea(data):
    return data


def forecastPublicArea(data):
    return data


def measuredBuildingArea(data):
    data = data.asDict()
    data['MeasuredBuildingArea'] = Meth.cleanUnit(data['MeasuredBuildingArea'])
    return Row(**data)


def measuredInsideOfBuildingArea(data):
    data = data.asDict()
    data['MeasuredInsideOfBuildingArea'] = Meth.cleanUnit(data['MeasuredInsideOfBuildingArea'])
    return Row(**data)


def measuredSharedPublicArea(data):
    data = data.asDict()
    data['MeasuredSharedPublicArea'] = Meth.cleanUnit(data['MeasuredSharedPublicArea'])
    return Row(**data)


def measuredUndergroundArea(data):
    return data


def toward(data):
    return data


def houseType(data):
    return data


def houseNature(data):
    return data


def decoration(data):
    return data


def natureOfPropertyRight(data):
    return data


def houseUseType(data):
    return data


def buildingStructure(data):
    data = data.asDict()
    data['BuildingStructure'] = Meth.cleanName(data['BuildingStructure']).replace('钢混','钢混结构')\
                                                     .replace('框架','框架结构')\
                                                     .replace('钢筋混凝土','钢混结构')\
                                                     .replace('混合','混合结构')\
                                                     .replace('结构结构','结构')\
                                                     .replace('砖混','砖混结构')\
                                                     .replace('框剪','框架剪力墙结构')\
                                                     .replace('钢、','')
    return Row(**data)


def houseSalePrice(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def isMortgage(data):
    return data


def isAttachment(data):
    return data


def isPrivateUse(data):
    return data


def isMoveback(data):
    return data


def isSharedPublicMatching(data):
    return data


def sellState(data):
    return data


def sellSchedule(data):
    return data


def houseState(data):
    return data


def houseStateLatest(data):
    return data


def houseLabel(data):
    return data


def houseLabelLatest(data):
    return data


def totalPrice(data):
    data = data.asDict()
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    area  = Meth.cleanUnit(data['MeasuredBuildingArea'])
    if price and area:
        data['TotalPrice'] = round(float(price) *float(area),2)
    else: 
        data['TotalPrice'] = ''
    return Row(**data)


def price(data):
    data = data.asDict()
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    if price:
        data['Price'] = price.group()
    else: 
        data['Price'] = ''
    return Row(**data)


def priceType(data):
    data = data.asDict()
    data['PriceType'] = '备案价格'.decode('utf-8')
    return Row(**data)


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJSON(data):
    return data
