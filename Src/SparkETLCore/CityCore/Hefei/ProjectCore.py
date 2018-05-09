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
reload(sys)
sys.setdefaultencoding('utf8')

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
    # print (data['ProjectName'].encode('utf-8'))
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    print (df.col.values[0].__str__())
    # data['HousingCount'] = df.col.values[0].__str__()
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
    data['RealEstateProjectID'] = data['ProjectID']
    return Row(**data)


def projectUUID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectUUID'] = data['ProjectUUID']
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
    unsoldNum = df['HouseID'][~df.HouseState.isin(["可售","抵押可售","摇号销售","现房销售"])].size
    if presaleNum and unsoldNum:
        data['OnSaleState'] = '售馨'.decode('utf-8') if (unsoldNum/presaleNum) < 0.1 else '在售'.decode('utf-8')
    return Row(**data)


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # data['HousingCount'] = df.col.values[0].__str__()
    # return Row(**data)
    return data


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
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select BuildingID,BuildingArea from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # if not df.empty:
    #     data['TotalBuidlingArea'] = df.groupby('BuildingID')\
    #                                     .apply(lambda x:x[:1])['BuildingArea']\
    #                                     .apply(Meth.cleanUnit)\
    #                                     .apply(lambda x:float(x) if x else 0.0)\
    #                                     .sum().__str__()
    # return Row(**data)
    data = data.asDict()
    if data['TotalBuidlingArea']:
        data['TotalBuidlingArea'] = Meth.cleanUnit(data['TotalBuidlingArea'])
    return Row(**data)

def buildingType(data):
    return data

def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    usageSet = df['ExtraJson'].apply(Meth.jsonLoad).apply(lambda x:x.get('BuildingUsage',''))
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
    return data


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select distinct(PresalePermitNumber) as col from BuildingInfoItem where ProjectUUID='{projectUUID}'"\
                     .format(projectUUID=data['ProjectUUID']))
    data['PresalePermitNumber'] =';'.join(set(df.col.values) - set(['']))
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
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select count(distinct(HouseID)) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # data['ApprovalPresaleAmount'] = str(df.col.values[0])
    # return Row(**data)
    return data

def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select HouseID,RecordTime,MeasuredBuildingArea from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # if not df.empty:
    #     df = df.sort_values(by='RecordTime',ascending=False)\
    #                             .groupby('HouseID')\
    #                             .apply(lambda x:x[:1])['MeasuredBuildingArea']                     
    #     data['ApprovalPresaleArea'] = df.apply(Meth.cleanUnit)\
    #                                             .apply(lambda x:float(x) if x else 0.0)\
    #                                             .sum().__str__()
    # return Row(**data) 
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
    if not df.empty:
        data['EarliestOpeningTime'] = df.ExtraJson.apply(Meth.jsonLoad)\
                                        .apply(lambda x:x.get('ExtraBuildingOpenDate'))\
                                        .apply(Meth.cleanUnit).replace('',method='bfill').min().__str__()
    return Row(**data)


def latestDeliversHouseDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql="select ExtraJson from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    if not df.empty:
        data['LatestDeliversHouseDate'] = df.ExtraJson.apply(Meth.jsonLoad).apply(lambda x:x.get('ExtraBuildingDeliverDate'))\
                                            .apply(Meth.cleanUnit).max().__str__()
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
    if not df.empty:
        df['ExtraBuildingAreaCode'] = df.ExtraJson.apply(Meth.jsonLoad).apply(lambda x:x.get('ExtraBuildingAreaCode'))\
                                         .apply(Meth.cleanName)
        data['CertificateOfUseOfStateOwnedLand'] =';'.join(df['ExtraBuildingAreaCode'][df.ExtraBuildingAreaCode!=''].unique())
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
    if not df.empty:
        df['ExtraBuildingPlanCode'] = df.ExtraJson.apply(Meth.jsonLoad).apply(lambda x:x.get('ExtraBuildingPlanCode'))\
                                         .apply(Meth.cleanName)
        data['BuildingPermit'] =';'.join(df['ExtraBuildingPlanCode'][df.ExtraBuildingPlanCode!=''].unique())
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
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select  HouseUseType,HouseID from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # if not df.empty:
    #     data['ParkingSpaceAmount'] = df['HouseID'][df.HouseUseType.str.contains(u'车')].unique().size.__str__() 
    # return Row(**data)
    return data

def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
