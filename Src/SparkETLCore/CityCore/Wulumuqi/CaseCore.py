# coding=utf-8
from __future__ import division
import sys
import re
import inspect
import pandas as pd
import numpy as np
import os 
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from SparkETLCore.Utils import  Meth, Config,Var
reload(sys)
sys.setdefaultencoding('utf8')

METHODS =   ['address',
     'balconys',
     'buildingCompletedYear',
     'buildingID',
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
     'houseID',
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
     'realEstateProjectID',
     'regionName',
     'remarks',
     'sellSchedule',
     'sellState',
     'sourceLink',
     'state',
     'totalPrice',
     'unenclosedBalconys',
     'unitShape',
     'unitStructure',
     'projectUUID',
     'buildingUUID',
     'houseUUID']


def projectUUID (data):
    data = data.asDict()
    data['ProjectUUID'] = data['ProjectUUID']
    return Row(**data)

def buildingUUID (data):
    data = data.asDict()
    data['BuildingUUID'] = data['BuildingUUID']
    return Row(**data)

def houseUUID (data):
    data = data.asDict()
    data['HouseUUID'] = data['HouseUUID']
    return Row(**data)

def realEstateProjectID (data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectID  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['RealEstateProjectID'] = df.col[df.col!=''].fillna('').values[0]
    return Row(**data)

def buildingID (data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select BuildingID  as col from BuildingInfoItem where BuildingUUID='{buildingUUID}'".format(
                         buildingUUID=data['BuildingUUID']))
    data['BuildingID'] = df.col[df.col!=''].fillna('').values[0]
    return Row(**data)

def houseID (data):
    return data

def forecastBuildingArea (data):
    return data

def forecastInsideOfBuildingArea (data):
    return data

def forecastPublicArea (data):
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

def isMortgage (data):
    return data

def isAttachment (data):
    return data

def isPrivateUse (data):
    return data

def isMoveBack (data):
    return data

def isSharedPublicMatching (data):
    return data

def buildingStructure (data):
    return data

def sellSchedule (data):
    return data

def sellState (data):
    return data

def sourceLink (data):
    return data

def caseTime (data):
    return data

def caseFrom (data):
    return data

def unitShape (data):
    return data

def unitStructure (data):
    return data

def balconys (data):
    return data

def unenclosedBalconys (data):
    return data

def districtName (data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select districtname  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['DistrictName'] = Meth.cleanName(df.col.values[0]).decode('utf-8')
    return Row(**data)

def regionName (data):
    return data

def projectName(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select (ProjectName)  as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress!='' "\
                     .format(projectUUID=data['ProjectUUID']))
    data['ProjectName'] = Meth.cleanName(df.col.values[0])
    return Row(**data)

def buildingName(data):
    data = data.asDict()
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)

def presalePermitNumber (data):
    data = data.asDict()
    data['PresalePermitNumber'] = Meth.cleanName(data['BuildingName'])
    return Row(**data)

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

def totalPrice (data):
    data = data.asDict()
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    area  = Meth.cleanUnit(data['MeasuredBuildingArea'])
    if price and area:
        # print (area)
        # print (price)
        data['TotalPrice'] = round(float(price.group()) *float(area),2)
    else: 
        data['TotalPrice'] = ''
    return Row(**data)

def price (data):
    data = data.asDict()
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    if price:
        data['Price'] = price.group()
    else: 
        data['Price'] = ''
    return Row(**data)

def priceType (data):
    data = data.asDict()
    data['PriceType'] = '备案价格'.decode('utf-8')
    return Row(**data)

def address(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['Address'] = Meth.cleanName(df.col.values[0])
    return Row(**data)

def buildingCompletedYear (data):
    data = data.asDict()
    data['BuildingCompletedYear'] =  ''
    return Row(**data)

def floor (data):
    data = data.asDict()
    data['ActualFloor'] =  data['FloorName'].decode('utf-8')
    return Row(**data)

def nominalFloor (data):
    return data

def floors (data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ProjectAddress  as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['Address'] = Meth.cleanName(df.col.values[0]).decode('utf-8')
    return Row(**data)

def houseUseType (data):
    return data

def dwelling (data):
    return data

def state (data):
    data = data.asDict()
    if data['SellState'] in ["可售","抵押可售","摇号销售","现房销售"] \
        and data['HouseStateLatest'] in ["现房销售","已签约","已备案","已办产权","网签备案单"]:
        data['State'] = '明确成交'.decode('utf-8')
    
    elif data['SellState'] in ["现房销售","已签约","已备案","已办产权","网签备案单"]\
        and data['HouseStateLatest'] in ["可售","抵押可售","摇号销售","现房销售"]:
        data['State'] = '明确退房'.decode('utf-8')
    
    elif data['SellState'] in ["可售","抵押可售","摇号销售","现房销售"]\
        and data['HouseStateLatest']=='':
        data['State'] = '明确供应'.decode('utf-8')
    else:
        pass
    return Row(**data)

def dealType (data):
    data = data.asDict()
    if data['SellState'] in ["可售","抵押可售","摇号销售","现房销售"] \
        and data['HouseStateLatest'] in ["现房销售","已签约","已备案","已办产权","网签备案单"]:
        data['DealType'] = '最新成交'
    return Row(**data)

def remarks (data):
    data = data.asDict()
    data['Remarks'] = ''
    return Row(**data)

