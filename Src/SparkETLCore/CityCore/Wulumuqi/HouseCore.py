# coding=utf-8
from __future__ import division
import sys
import re
import os 
import inspect
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from SparkETLCore.Utils import  Meth, Config,Var
reload(sys)
sys.setdefaultencoding('utf8')

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
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select (ProjectName)  as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress!='' "\
                     .format(projectUUID=data['ProjectUUID']))
    data['ProjectName'] = Meth.cleanName(df.col.values[0])
    return Row(**data)


def realEstateProjectId(data):
    return data


def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)


def buildingId(data):
    return data


def city(data):
    data = data.asDict()
    data['City'] = '乌鲁木齐'.decode('utf-8')
    return Row(**data)


def districtname(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select DistrictName as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and DistrictName !=''".format(
                         projectUUID=data['ProjectUUID']))
    if not df.empty:
        data['DistrictName'] = Meth.cleanName(df.col.values[0])
    return Row(**data)

def unitName(data):
    data = data.asDict()
    data['UnitName'] = Meth.cleanName(data['UnitName'])
    return Row(**data)

def unitId(data):
    return data


def houseNumber(data):
    data = data.asDict()
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return Row(**data)

def houseName(data):
    data = data.asDict()
    data['HouseName'] =  data['UnitName']+Meth.cleanName(data['HouseNumber']).decode('utf-8')
    return Row(**data)


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress  as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress!=''".format(
                         projectUUID=data['ProjectUUID']))
    if not df.empty:
        data['Address'] = Meth.cleanName(df.col.values[0])
    return Row(**data)

def floorName(data):
    data = data.asDict()
    data['FloorName'] =  data['FloorName'].decode('utf-8')+u'层'
    return Row(**data)

def actualFloor(data):
    return data

def floorCount(data):
    return data


def floorType(data):
    return data

def floorRight(data):
    return data


def unitShape(data):
    return data

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
    return data


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
    return data


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
    return data


def price(data):
    return data


def priceType(data):
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJSON(data):
    return data
