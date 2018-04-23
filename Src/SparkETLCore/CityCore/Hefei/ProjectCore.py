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

def test(data):
    data = data.asDict()
    print (data['ProjectName'])
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])+'1231'
    print (data['ProjectName'])
    return Row(**data)
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
    data = data.asDict()
    data['RealEstateProjectID'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraProjectID', ''))
    return Row(**data)


def projectUUID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectUUID'] = data['ProjectID']
    return Row(**data)


def districtName(data):
    # print(data, inspect.stack()[0][3])
    return data


def regionName(data):
    # print(data, inspect.stack()[0][3])
    return data


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
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseUUID,HouseState,HouseID from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    presaleNum = df['HouseID'].size
    unsoldNum = df['HouseID'][~HouseState.isin(["可售","抵押可售","摇号销售","现房销售"])].size
    if presaleNum and unsoldNum:
        data['OnSaleState'] = '售馨'.decode('utf-8') if (unsoldNum/presaleNum) < 0.1 else '在售'.decode('utf-8')
    return Row(**data)


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['HousingCount'] = df.col.values[0].__str__()
    return Row(**data)


def developer(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer']).decode('utf-8')
    return Row(**data)


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select BuildingID,TotalBuidlingArea from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['TotalBuidlingArea'] = df['TotalBuidlingArea'].groupby('BuildingID')\
                                    .apply(lambda x:x[:1])\
                                    .apply(Meth.cleanUnit)\
                                    .apply(lambda x:float(x) if x else 0.0)\
                                    .sum().__str__()
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
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct(FloorName) from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    floorNum = df['FloorName'].apply(lambda x:float(x) if x else 1).max()
    data['BuildingType'] = check_floor_type(floorNum).decode('utf-8')
    return Row(**data)

def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    usageSet = pd['ExtraJson'].apply(Meth.jsonLoad).apply(lambda x:x.get('BuildingUsage',''))
    data['HouseUseType'] = ';'.join((set(usageSet) - set([''])))
    return Row(**data)

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
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['ApprovalPresaleAmount'] = str(df.col.values[0])
    return data


def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select HouseID,RecordTime,MeasuredBuildingArea from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    df = df.sort_values(by='RecordTime',ascending=False)\
                            .groupby('HouseID')\
                            .apply(lambda x:x[:1])['MeasuredBuildingArea']                     
    data['ApprovalPresaleArea'] = df.apply(Meth.cleanUnit)\
                                            .apply(lambda x:float(x) if x else 0.0)\
                                            .sum().__str__()
    return data


def averagePrice(data):
    # print(data, inspect.stack()[0][3])
    return data


def earliestStartDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def completionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def earliestOpeningTime(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['EarliestOpeningTime'] = BuildDF['ExtraJson']\
                                    .apply(lambda x:x.get('ExtraBuildingOpenDate'))\
                                    ['ExtraBuildingOpenDate'][a.ExtraBuildingOpenDate!='']\
                                    .apply(Meth.cleanUnit).min().__str__()
    return Row(**data)


def latestDeliversHouseDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['EarliestOpeningTime'] = BuildDF.ExtraJson.apply(lambda x:x.get('ExtraBuildingDeliverDate'))\
                                     ['ExtraBuildingDeliverDate'].apply(Meth.cleanUnit).max().__str__()
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
    # print(data, inspect.stack()[0][3])
    return data


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    cd = BuildDF.ExtraJson.apply(lambda x:x.get('ExtraBuildingAreaCode'))\
                                     ['ExtraBuildingAreaCode'].apply(Meth.cleanName).unique()
    data['CertificateOfUseOfStateOwnedLand'] =list(cd)
    return Row(**data)


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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    bt = BuildDF.ExtraJson.apply(lambda x:x.get('ExtraBuildingPlanCode'))\
                                     ['ExtraBuildingPlanCode'].apply(Meth.cleanName).unique()
    data['BuildingPermit'] =list(bt)
    return Row(**data)


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
    return data


def parkingSpaceAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select  HouseUseType,HouseID from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['ParkingSpaceAmount'] = df['HouseID'][HouseDF.HouseUseType.str.contains('车')]\
                                    .unique().__len__().__str__() 
    return Row(**data)


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
