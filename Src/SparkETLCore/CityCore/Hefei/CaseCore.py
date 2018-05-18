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

METHODS =   [
     'recordTime',
     'address',
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
     'actualFloor',
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
     'sourceUrl',
     'state',
     'totalPrice',
     'unenclosedBalconys',
     'unitShape',
     'unitStructure',
     'projectUUID',
     'buildingUUID',
     'houseUUID',
     'projectID',
     'buildingID',
     'houseID']


def projectUUID (data):
    data['ProjectUUID'] = data['ProjectUUID']
    return data

def buildingUUID (data):
    data['BuildingUUID'] = data['BuildingUUID']
    return data

def houseUUID (data):
    data['HouseUUID'] = data['HouseUUID']
    return data

def unitUUID(data):
    return data

def recordTime(data):
    return data

def projectID(data):
    data['ProjectID']=''
    return data

def realEstateProjectID (data):
    return data

def buildingID (data):
    data['BuildingID']=''
    return data

def houseID (data):
    return data

def forecastBuildingArea (data):
    return data

def forecastInsideOfBuildingArea (data):
    return data

def forecastPublicArea (data):
    return data

def measuredBuildingArea(data):
    if data['MeasuredBuildingArea']:
        data['MeasuredBuildingArea'] = Meth.cleanUnit(data['MeasuredBuildingArea'])
    return data

def measuredInsideOfBuildingArea(data):
    if data['MeasuredInsideOfBuildingArea']:
        data['MeasuredInsideOfBuildingArea'] = Meth.cleanUnit(data['MeasuredInsideOfBuildingArea'])
    return data

def measuredSharedPublicArea(data):
    if data['MeasuredSharedPublicArea']:
        data['MeasuredSharedPublicArea'] = Meth.cleanUnit(data['MeasuredSharedPublicArea'])
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
    if data['BuildingStructure']: 
        data['BuildingStructure'] = Meth.cleanName(data['BuildingStructure']).replace('钢混','钢混结构')\
                                                     .replace('框架','框架结构')\
                                                     .replace('钢筋混凝土','钢混结构')\
                                                     .replace('混合','混合结构')\
                                                     .replace('结构结构','结构')\
                                                     .replace('砖混','砖混结构')\
                                                     .replace('框剪','框架剪力墙结构')\
                                                     .replace('钢、','')
    return data

def sellSchedule (data):
    return data

def sellState (data):
    data['SellState'] = data['HouseState']
    return data

def sourceUrl (data):
    return data

def caseTime (data):
    data['CaseTime'] = data['RecordTime'] 
    return data

def caseFrom (data):
    data['CaseFrom'] = '合肥市房产管理网'
    return data

def unitShape (data):
    data['UnitShape'] = Meth.jsonLoad(data['ExtraJson']).get('ExtraHouseType','')
    return data

def unitStructure (data):
    return data

def balconys (data):
    return data

def unenclosedBalconys (data):
    return data

def districtName (data):
    if data['ProDistrictName']:
        data['DistrictName'] = Meth.cleanName(data['ProDistrictName'])
    return data

def regionName (data):
    data['RegionName'] =''
    return data

def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data

def buildingName(data):
    data['BuildingName'] = Meth.cleanName(data['BuildingName'])
    return data

def presalePermitNumber (data):
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return data

def houseNumber(data):
    data['HouseNumber'] = Meth.cleanName(data['HouseNumber'])
    return data

def houseName(data):
    data['HouseName'] =  data['FloorName']\
                        +'层'\
                        +Meth.cleanName(data['HouseNumber'])
    return data

def totalPrice (data):
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    area  = Meth.cleanUnit(data['MeasuredBuildingArea'])
    if price and area:
        data['TotalPrice'] = round(float(price.group()) *float(area),2)
    else: 
        data['TotalPrice'] = ''
    return data

def price (data):
    rule = re.compile('\d+\.?\d+')
    price = rule.search(Meth.jsonLoad(data['ExtraJson']).get('ExtraHousePreSellPrice',''))
    if price:
        data['Price'] = price.group()
    else: 
        data['Price'] = ''
    return data

def priceType (data):
    data['PriceType'] = '备案价格'
    return data

def address(data):
    if data['ProProjectAddress']:
        data['Address'] =  Meth.cleanName(data['ProProjectAddress'])
    return data

def buildingCompletedYear (data):
    data['BuildingCompletedYear'] =  ''
    return data

def actualFloor (data):
    data['ActualFloor'] =  data['FloorName']
    return data

def nominalFloor (data):
    data['NominalFloor']=''
    return data

def floors (data):
    data['Floors'] =''
    return data

def houseUseType (data):
    return data

def dwelling (data):
    data['Dwelling'] =''
    return data

def state (data):
    if data['HouseState'] in ["现房销售","已签约","已备案","已办产权","网签备案单"] \
        and data['HouseStateLatest'] in ["可售","抵押可售","摇号销售","现房销售"]:
        data['State'] = '明确成交'
    
    elif data['HouseState'] in ["可售","抵押可售","摇号销售","现房销售"]\
        and data['HouseStateLatest'] in ["现房销售","已签约","已备案","已办产权","网签备案单"]:
        data['State'] = '明确退房'
    
    elif data['HouseState'] in ["可售","抵押可售","摇号销售","现房销售"]\
        and data['HouseStateLatest']=='':
        data['State'] = '明确供应'
    else:
        pass
    return data

def dealType (data):
    data['DealType'] =''
    if data['HouseState'] in ["现房销售","已签约","已备案","已办产权","网签备案单"]\
        and data['HouseStateLatest'] in ["可售","抵押可售","摇号销售","现房销售"]:
        data['DealType'] = '最新成交'
    return data

def remarks (data):
    data['Remarks'] = ''
    return data

