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

METHODS = ['approvalPresaleAmount',
           'approvalPresaleArea',
           'averagePrice',
           'buildingPermit',
           'buildingType',
           'certificateOfUseOfStateOwnedLand',
           'completionDate',
           'constructionPermitNumber',
           'decoration',
           'developer',
           'districtName',
           'earliestOpeningTime',
           'earliestStartDate',
           'extraJson',
           'floorArea',
           'floorAreaRatio',
           'greeningRate',
           'houseBuildingCount',
           'houseUseType',
           'housingCount',
           'landLevel',
           'landUse',
           'landUsePermit',
           'latestDeliversHouseDate',
           'legalPerson',
           'legalPersonNumber',
           'lssueDate',
           'managementCompany',
           'managementFees',
           'onSaleState',
           'otheRights',
           'parkingSpaceAmount',
           'presalePermitNumber',
           'presaleRegistrationManagementDepartment',
           'projectAddress',
           'projectApproveData',
           'projectBookingdData',
           'projectName',
           'projectType',
           'projectUUID',
           'promotionName',
           'propertyRightsDescription',
           'qualificationNumber',
           'realEstateProjectId',
           'recordTime',
           'regionName',
           'remarks',
           'sourceUrl',
           'totalBuidlingArea']


def recordTime(data):
    # print(data, inspect.stack()[0][3])
    return data

def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def promotionName(data):
    # print(data, inspect.stack()[0][3])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectUUID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectUUID'] = data['ProjectID']
    return Row(**data)


def districtName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return Row(**data)


def regionName(data):
    data = data.asDict()
    if data['RegionName']:  
        data['RegionName'] = Meth.cleanName(data['RegionName'])
    return Row(**data)


def projectAddress(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
    return Row(**data)


def projectType(data):
    # print(data, inspect.stack()[0][3])
    return data


def onSaleState(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    transferDT =  { "在售":'在售',
                    "待售":'',
                    "尾盘":'售馨'}
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseUUID,HouseState,HouseID from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    presaleNum = df['HouseID'].size
    unsoldNum = df['HouseID'][~HouseState.isin(["可售"])].size
    if presaleNum and unsoldNum:
        data['OnSaleState'] = '售馨'.decode('utf-8') if (unsoldNum/presaleNum) < 0.1 else '在售'.decode('utf-8')
    else:
        data['OnSaleState'] = transferDT.get(data['OnSaleState'])
    return Row(**data)


def landUse(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['LandUse'] = Meth.cleanName(LandUse)
    return Row(**data)


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    rule = re.complie(r'\d+')
    hc = rule.findall(data['HousingCount'])
    if hc:
        data['HouseCount'] = sum(map(lambda x:int(x) if x ,a)).__str__()
    return Row(**data)

def developer(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer'])
    return Row(**data)


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorArea'] = Meth.cleanUnit(data['FloorArea'])
    return Row(**data)


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    rule = re.compile(r'\d+.?\d+')
    data['TotalBuidlingArea'] = Meth.cleanUnit(data['TotalBuidlingArea'])
    if data['TotalBuidlingArea']:
        ta = rule.search(data['TotalBuidlingArea'])
        if ta:
            data['TotalBuidlingArea'] = ta.group()
    return Row(**data)


def buildingType(data):
    return data

def houseUseType(data):
    return data

def propertyRightsDescription(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectApproveData(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectBookingdData(data):
    # print(data, inspect.stack()[0][3])
    return data


def lssueDate(data):
    # print(data, inspect.stack()[0][3])
    return Row(**data)


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct(PresalePermitNumber) as col from BuildingInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['PresalePermitNumber'] =';'.join(list(set(df.col.values) - set([''])))
    return Row(**data)


def houseBuildingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['HouseBuildingCount'] = len(list(set(df.col.values) - set(['']))).__str__()
    return Row(**data)


def approvalPresaleAmount(data):
    return data


def approvalPresaleArea(data):
    return data


def averagePrice(data):
    # print(data, inspect.stack()[0][3])
    return data


def earliestStartDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def completionDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    if data['CompletionDate']:
        data['CompletionDate'] =Meth.cleanDate(data['CompletionDate'])
    return Row(**data)


def earliestOpeningTime(data):
    # print(data, inspect.stack()[0][3])
    return data


def latestDeliversHouseDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    if data['LatestDeliversHouseDate']:
        data['LatestDeliversHouseDate'] =Meth.cleanDate(data['LatestDeliversHouseDate'])
    return Row(**data)


def presaleRegistrationManagementDepartment(data):
    # print(data, inspect.stack()[0][3])
    return data


def landLevel(data):
    # print(data, inspect.stack()[0][3])
    return data


def greeningRate(data):
    data = data.asDict()
    data['GreeningRate'] = Meth.cleanUnit(data['GreeningRate'])
    # print(data, inspect.stack()[0][3])
    return Row(**data)


def floorAreaRatio(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorAreaRatio'] = Meth.cleanUnit(data['FloorAreaRatio'])
    # print(data, inspect.stack()[0][3])
    return Row(**data)



def managementFees(data):
    data = data.asDict()
    data['ManagementFees'] = Meth.cleanUnit(data['ManagementFees'].replace('暂无资料'))
    # print(data, inspect.stack()[0][3])
    return Row(**data)



def managementCompany(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ManagementCompany'] = Meth.cleanName(data['ManagementCompany'])
    # print(data, inspect.stack()[0][3])
    return Row(**data)


def otheRights(data):
    # print(data, inspect.stack()[0][3])
    return data


def certificateOfUseOfStateOwnedLand(data):
    # print(data, inspect.stack()[0][3])
    return data


def constructionPermitNumber(data):
    # print(data, inspect.stack()[0][3])
    return data


def qualificationNumber(data):
    # print(data, inspect.stack()[0][3])
    return data


def landUsePermit(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPermit(data):
    # print(data, inspect.stack()[0][3])
    return data


def legalPersonNumber(data):
    # print(data, inspect.stack()[0][3])
    return data


def legalPerson(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    return data


def decoration(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Decoration'] = Meth.cleanName(data['Decoration'])
    # print(data, inspect.stack()[0][3])
    return Row(**data)


def parkingSpaceAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ParkingSpaceAmount'] =data['ParkingSpaceAmount'].replace('个','')
    return Row(**data)


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
