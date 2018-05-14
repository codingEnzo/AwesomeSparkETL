# coding=utf-8
from __future__ import division
import sys
import inspect
import pandas as pd
import numpy as np
import datetime
from pyspark.sql import Row

from SparkETLCore.Utils import Var, Meth, Config

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
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def promotionName(data):
    return data


def realEstateProjectId(data):
    return data


def projectUUID(data):
    return data


def districtName(data):
    return data


def regionName(data):
    return data


def projectAddress(data):
    return data


def projectType(data):
    return data


def onSaleState(data):
    unsoldNum = int(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraProjectSaleNum', '0'))
    if unsoldNum == 0:
        data['OnSaleState'] = '售馨'
    else:
        data['OnSaleState'] = '在售'
    return data


def landUse(data):
    return data


def housingCount(data):
    return data


def developer(data):
    data['Developer'] = Meth.cleanName(data['Developer'])
    return data


def floorArea(data):
    return data


def totalBuidlingArea(data):
    ProjectSaleArea = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectSaleArea', 0.00))
    ProjectSaledArea = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectSaledArea', 0.00))
    data['TotalBuidlingArea'] = ProjectSaleArea + ProjectSaledArea
    return data


def buildingType(data):
    return data


def houseUseType(data):
    data['HouseUseType'] = Meth.jsonDumps(data['HouseUseType'].split('@#$'))
    return data


def propertyRightsDescription(data):
    return data


def projectApproveData(data):
    return data


def projectBookingdData(data):
    return data


def lssueDate(data):
    # 发证日期

    return data


def presalePermitNumber(data):
    # 预售正编号

    return data


def houseBuildingCount(data):
    #
    # df = pd.read_sql(con=Var.ENGINE,
    #                  sql=u"select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(projectUUID=data['ProjectUUID']))
    # data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
    return data


def approvalPresaleAmount(data):
    # 批准预售套数

    return data


def approvalPresaleArea(data):
    # 批准预售面积

    return data


def averagePrice(data):
    # 均价

    data['AveragePrice'] = data['AveragePrice']
    return data


def earliestStartDate(data):
    return data


def completionDate(data):
    return data


def earliestOpeningTime(data):
    return data


def latestDeliversHouseDate(data):
    # 最晚交房时间

    return data


def presaleRegistrationManagementDepartment(data):
    # 预售登记管理备案部门

    return data


def landLevel(data):
    # 土地登记

    return data


def greeningRate(data):
    return data


def floorAreaRatio(data):
    return data


def managementFees(data):
    return data


def managementCompany(data):
    return data


def otheRights(data):
    return data


def certificateOfUseOfStateOwnedLand(data):
    data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(
        data['CertificateOfUseOfStateOwnedLand'])
    return data


def constructionPermitNumber(data):
    # 施工许可证号

    data['ConstructionPermitNumber'] = Meth.cleanName(
        data['ConstructionPermitNumber'])
    return data


def qualificationNumber(data):
    # 资质证编号

    return data


def landUsePermit(data):
    return data


def buildingPermit(data):
    data['BuildingPermit'] = Meth.cleanName(data['BuildingPermit'])
    return data


def legalPersonNumber(data):
    return data


def legalPerson(data):
    return data


def sourceUrl(data):
    data['SourceUrl'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraSourceURL', ''))
    return data


def decoration(data):
    # 装修

    return data


def parkingSpaceAmount(data):
    data['ParkingSpaceAmount'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraParkingTotalSoldAmount', ''))
    return data


def remarks(data):
    return data


def extraJson(data):
    return data
