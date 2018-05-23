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
           'totalBuidlingArea',
           'projectID']


def recordTime(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectID(data):
    # print(data, inspect.stack()[0][3])
    return data

def projectName(data):
    # print(data, inspect.stack()[0][3])
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def promotionName(data):
    # print(data, inspect.stack()[0][3])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectUUID(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    return data


def districtName(data):
    # print(data, inspect.stack()[0][3])
    return data


def regionName(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectAddress(data):
    # print(data, inspect.stack()[0][3])
    data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
    return data


def projectType(data):
    # print(data, inspect.stack()[0][3])
    return data


def onSaleState(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select HouseUUID,HouseState,HouseID from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # presaleNum = df['HouseID'].size
    # unsoldNum = df['HouseID'][~df.HouseState.isin(["可售","抵押可售","摇号销售","现房销售"])].size
    # if presaleNum and unsoldNum:
    #     data['OnSaleState'] = '售馨'.decode('utf-8') if (unsoldNum/presaleNum) < 0.1 else '在售'.decode('utf-8')
    return data


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    return data


def developer(data):
    # print(data, inspect.stack()[0][3])
    data['Developer'] = Meth.cleanName(data['Developer']).decode('utf-8')
    return data


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def totalBuidlingArea(data):
    data['TotalBuidlingArea'] = Meth.cleanUnit(data['TotalBuidlingArea'])
    cache = set(Meth.cleanUnit(data['BuiTotalBuidlingArea']).split('@#$'))-set([''])
    if not data['TotalBuidlingArea']:
        data['TotalBuidlingArea'] = sum(list(map(float,cache))).__str__()
    return data

def buildingType(data):
    return data

def houseUseType(data):
    if data['BuiExtraJson']:
        cache = map(lambda x:Meth.cleanName(Meth.jsonLoad(x).get('ExtraBuildingUsage','')) if x else ''
                    ,data['BuiExtraJson'].split('@#$'))
        if set(cache)-set(['']):
            data['HouseUseType'] = Meth.jsonDumps(list(set(cache)-set([''])))
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
    return data


def presalePermitNumber(data):
    if data['BuiPresalePermitNumber']:
        data['BuiPresalePermitNumber'] = Meth.cleanName(data['BuiPresalePermitNumber'])
        data['PresalePermitNumber'] = Meth.jsonDumps(
                                            list(set(data['BuiPresalePermitNumber'].split('@#$'))))
    return data


def houseBuildingCount(data):
    data['HouseBuildingCount'] = data['BuiHouseBuildingCount'].__str__()
    return data


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
    return data


def earliestOpeningTime(data):
    # print(data, inspect.stack()[0][3])
    if data['BuiExtraJson']:
        cache = map(lambda x:Meth.jsonLoad(x).get('ExtraBuildingOpenDate','') if x else ''
                    ,data['BuiExtraJson'].split('@#$'))
        if set(cache) - set(['']):
            data['EarliestOpeningTime'] = min(set(cache) - set([''])).replace('/','-')

    return data


def latestDeliversHouseDate(data):
    # print(data, inspect.stack()[0][3])
    if data['BuiExtraJson']:
        cache = map(lambda x:Meth.jsonLoad(x).get('ExtraBuildingDeliverDate','') if x else ''
                    ,data['BuiExtraJson'].split('@#$'))
        if set(cache) - set(['']):
            data['LatestDeliversHouseDate'] = max(set(cache)).replace('/','-')
    return data


def presaleRegistrationManagementDepartment(data):
    # print(data, inspect.stack()[0][3])
    return data


def landLevel(data):
    # print(data, inspect.stack()[0][3])
    return data


def greeningRate(data):
    # data = data.asDict()
    data['GreeningRate'] = Meth.cleanUnit(data['GreeningRate'])
    # print(data, inspect.stack()[0][3])
    return data


def floorAreaRatio(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    data['FloorAreaRatio'] = Meth.cleanUnit(data['FloorAreaRatio'])
    # print(data, inspect.stack()[0][3])
    return data



def managementFees(data):
    # print(data, inspect.stack()[0][3])
    return data


def managementCompany(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    data['ManagementCompany'] = Meth.cleanName(data['ManagementCompany'])
    # print(data, inspect.stack()[0][3])
    return data


def otheRights(data):
    # print(data, inspect.stack()[0][3])
    return data


def certificateOfUseOfStateOwnedLand(data):

    if data['BuiExtraJson']:
        cache = map(lambda x:Meth.cleanName(Meth.jsonLoad(x).get('ExtraBuildingAreaCode','')) if x else ''
                    ,data['BuiExtraJson'].split('@#$'))
        if set(cache)-set(['']):
            data['CertificateOfUseOfStateOwnedLand'] = Meth.jsonDumps((list(set(cache)-set(['']))))
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
    if data['BuiExtraJson']:
        cache = map(lambda x:Meth.cleanName(Meth.jsonLoad(x).get('ExtraBuildingPlanCode','')) if x else ''
                    ,data['BuiExtraJson'].split('@#$'))
        if set(cache)-set(['']):
            data['BuildingPermit'] = Meth.jsonDumps((list(set(cache)-set(['']))))
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
    return data


def parkingSpaceAmount(data):
    # print(data, inspect.stack()[0][3])
    return data

def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
