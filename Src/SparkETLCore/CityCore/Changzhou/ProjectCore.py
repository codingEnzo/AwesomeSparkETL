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


def recordTime(spark, data):
    nowtime = datetime.datetime.now()
    if data['RecordTime'] == '':
        data['RecordTime'] = nowtime
    return data


def projectName(spark, data):
    return data


def promotionName(spark, data):
    return data


def realEstateProjectId(spark, data):
    return data


def projectUUID(spark, data):
    return data


def districtName(spark, data):
    return data


def regionName(spark, data):
    return data


def projectAddress(spark, data):
    return data


def projectType(spark, data):
    return data


def onSaleState(spark, data):
    return data


def landUse(spark, data):
    return data


def housingCount(spark, data):
    extraInfo = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectRecordsInfo', ''))
    housenum = [int(x.get('HouseTotalNum', 0)) for x in eval(extraInfo)]
    data['HousingCount'] = sum(housenum)
    return data


def developer(spark, data):
    return data


def floorArea(spark, data):
    return data


def totalBuidlingArea(spark, data):
    extraInfo = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectRecordsInfo', ''))
    housearea = [float(x.get('HouseTotalArea', 0.00)) for x in eval(extraInfo)]
    data['otalBuidlingArea'] = sum(housearea)
    return data


def buildingType(spark, data):
    return data


def houseUseType(spark, data):
    extraInfo = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectRecordsInfo', ''))
    HouseUseType = [x.get('HouseUsage', '') for x in eval(extraInfo)]
    data['HouseUseType'] = u','.join(list(set(HouseUseType)))
    return data


def propertyRightsDescription(spark, data):
    return data


def projectApproveData(spark, data):
    return data


def projectBookingdData(spark, data):
    return data


def lssueDate(spark, data):
    # 发证日期

    return data


def presalePermitNumber(spark, data):
    # 预售正编号
    sql = u"select ExtraJson from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
        projectUUID=data['ProjectUUID'])
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['PresalePermitNumber'] = query.apply(lambda x: Meth.jsonLoad(
            x['ExtraJson']).get('ExtraPresalePermitNumber', ''), axis=1)
        data['PresalePermitNumber'] = ','.join(list(set(query['PresalePermitNumber'])))
    return data


def houseBuildingCount(spark, data):
    sql = "select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
        projectUUID=data['ProjectUUID'])
    query = spark.sql(sql).toPandas()
    if not query.empty:
        data['HouseBuildingCount'] = str(len(list(set(query.col.values) - set(['']))))
    return data


def approvalPresaleAmount(spark, data):
    # 批准预售套数
    return data


def approvalPresaleArea(spark, data):
    # 批准预售面积
    return data


def averagePrice(spark, data):
    # 均价
    return data


def earliestStartDate(spark, data):
    return data


def completionDate(spark, data):
    return data


def earliestOpeningTime(spark, data):
    return data


def latestDeliversHouseDate(spark, data):
    # 最晚交房时间

    return data


def presaleRegistrationManagementDepartment(spark, data):
    # 预售登记管理备案部门
    return data


def landLevel(spark, data):
    # 土地登记
    return data


def greeningRate(spark, data):
    return data


def floorAreaRatio(spark, data):
    return data


def managementFees(spark, data):
    return data


def managementCompany(spark, data):
    return data


def otheRights(spark, data):
    return data


def certificateOfUseOfStateOwnedLand(spark, data):
    return data


def constructionPermitNumber(spark, data):
    # 施工许可证号
    return data


def qualificationNumber(spark, data):
    # 资质证编号
    return data


def landUsePermit(spark, data):
    return data


def buildingPermit(spark, data):
    return data


def legalPersonNumber(spark, data):
    return data


def legalPerson(spark, data):
    return data


def sourceUrl(spark, data):
    return data


def decoration(spark, data):
    # 装修
    return data


def parkingSpaceAmount(spark, data):
    return data


def remarks(spark, data):
    return data


def extraJson(spark, data):
    return data
