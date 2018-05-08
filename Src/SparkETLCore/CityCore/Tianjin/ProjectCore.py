# coding=utf-8
from __future__ import division
import sys
import inspect
import pandas as pd
import numpy as np
import datetime
from pyspark.sql import Row

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
    data = data.asDict()
    nowtime = str(datetime.datetime.now())
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(data):
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def promotionName(data):
    data = data.asDict()
    data['PromotionName'] = Meth.cleanName(data['PromotionName'])
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
    return data


def landUse(data):
    return data


def housingCount(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"SELECT sum(c.HousingCount) as col FROM (select a.BuildingName,a.HousingCount,a.ProjectName from BuildingInfoItem a where City = '天津' AND RecordTime = (select max(b.RecordTime) from BuildingInfoItem b where City = '天津' AND b.BuildingName = a.BuildingName) order by a.BuildingName) as cwhere c.ProjectName='{}';".format(
                         projectName=data['ProjectName']))
    data['HousingCount'] = df.col.values[-1] if not df.empty else ''


def developer(data):
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer'])
    return data


def floorArea(data):
    return data


def totalBuidlingArea(data):
    data = data.asDict()
    TotalBuidlingArea = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraTotalBuidlingArea', ''))
    data['TotalBuidlingArea'] = TotalBuidlingArea
    return data


def buildingType(data):
    return data


def houseUseType(data):
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
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql=u"select distinct(BuildingName) as col from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
    return data


def approvalPresaleAmount(data):
    # 批准预售套数

    return data


def approvalPresaleArea(data):
    # 批准预售面积

    return data


def averagel(data):
    # 均价
    return data


def earliestStartDate(data):
    return data


def completionDate(data):
    return data


def earliestOpeningTime(data):
    data = data.asDict()
    EarliestOpeningTime = float(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraEarliestOpeningTime', ''))
    data['EarliestOpeningTime'] = EarliestOpeningTime
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
    return data


def constructionPermitNumber(data):
    # 施工许可证号
    return data


def qualificationNumber(data):
    # 资质证编号

    return data


def landUsePermit(data):
    return data


def buildingPermit(data):
    return data


def legalPersonNumber(data):
    return data


def legalPerson(data):
    return data


def sourceUrl(data):
    return data


def decoration(data):
    # 装修
    return data


def parkingSpaceAmount(data):
    data = data.asDict()
    data['ParkingSpaceAmount'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraParkingTotalSoldAmount', ''))
    return data


def remarks(data):
    return data


def extraJson(data):
    return data
