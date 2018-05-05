# coding=utf-8
from __future__ import division
import sys
import inspect
import pandas as pd
import numpy as np
import datetime
from pyspark.sql import Row
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from Utils import Var, Meth, Config

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
    data = data.asDict()
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
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
    return data


def districtName(data):
    # print(data, inspect.stack()[0][3])
    return data


def regionName(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectAddress(data):
    # print(data, inspect.stack()[0][3])
    return data


def projectType(data):
    # print(data, inspect.stack()[0][3])
    return data


def onSaleState(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    unsoldNum = int(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraProjectSaleNum', '0'))
    if unsoldNum == 0:
        data['OnSaleState'] = '售馨'.decode('utf-8')
    else:
        data['OnSaleState'] = '在售'.decode('utf-8')
    return data


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    # data = data.asDict()
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql="select count(MeasuredBuildingArea) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
    #                      projectUUID=data['ProjectUUID']))
    # data['HousingCount'] = str(df.col.values[0])
    # return data
    return data

def developer(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer'])
    return data


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    ProjectSaleArea = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectSaleArea', 0.00))
    ProjectSaledArea = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectSaledArea', 0.00))
    data['TotalBuidlingArea'] = ProjectSaleArea + ProjectSaledArea
    return data


def buildingType(data):
    return data


def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.MIRROR_ENGINE,
                     sql=u"select distinct(HouseUseType) as col from house_info_dongguan where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['HouseUseType'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
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
    # 发证日期
    # print(data, inspect.stack()[0][3])
    return data


def presalePermitNumber(data):
    # 预售正编号
    # print(data, inspect.stack()[0][3])
    return data


def houseBuildingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(projectUUID=data['ProjectUUID']))
    data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
    return data


def approvalPresaleAmount(data):
    # 批准预售套数
    # print(data, inspect.stack()[0][3])
    return data


def approvalPresaleArea(data):
    # 批准预售面积
    # print(data, inspect.stack()[0][3])
    return data


def averagePrice(data):
    # 均价
    # print(data, inspect.stack()[0][3])
    data['averageprice'] = data['averageprice']
    return data


def earliestStartDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def completionDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def earliestOpeningTime(data):
    # print(data, inspect.stack()[0][3])
    return data


def latestDeliversHouseDate(data):
    # 最晚交房时间
    # print(data, inspect.stack()[0][3])
    return data


def presaleRegistrationManagementDepartment(data):
    # 预售登记管理备案部门
    # print(data, inspect.stack()[0][3])
    return data


def landLevel(data):
    # 土地登记
    # print(data, inspect.stack()[0][3])
    return data


def greeningRate(data):
    # print(data, inspect.stack()[0][3])
    return data


def floorAreaRatio(data):
    # print(data, inspect.stack()[0][3])
    return data


def managementFees(data):
    # print(data, inspect.stack()[0][3])
    return data


def managementCompany(data):
    # print(data, inspect.stack()[0][3])
    return data


def otheRights(data):
    # print(data, inspect.stack()[0][3])
    return data


def certificateOfUseOfStateOwnedLand(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(
        data['CertificateOfUseOfStateOwnedLand'])
    return data


def constructionPermitNumber(data):
    # 施工许可证号
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ConstructionPermitNumber'] = Meth.cleanName(
        data['ConstructionPermitNumber'])
    return data


def qualificationNumber(data):
    # 资质证编号
    # print(data, inspect.stack()[0][3])
    return data


def landUsePermit(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPermit(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingPermit'] = Meth.cleanName(data['BuildingPermit'])
    return data


def legalPersonNumber(data):
    # print(data, inspect.stack()[0][3])
    return data


def legalPerson(data):
    # print(data, inspect.stack()[0][3])
    return data


def sourceUrl(data):
    # print(data, inspect.stack()[0][3])
    data['sourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceURL', ''))
    return data


def decoration(data):
    # 装修
    # print(data, inspect.stack()[0][3])
    return data


def parkingSpaceAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ParkingSpaceAmount'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraParkingTotalSoldAmount', ''))
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
