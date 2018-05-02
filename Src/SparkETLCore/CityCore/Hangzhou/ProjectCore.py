# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

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
    # print(data, inspect.stack()[0][3])
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def promotionName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PromotionName'] = Meth.cleanName(data['PromotionName'])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraPropertyID', ''))
    return Row(**data)


def projectUUID(data):
    # print(data, inspect.stack()[0][3])
    return data


def districtName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return Row(**data)


def regionName(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
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
    status_dict = {
        '售完': '售罄',
        '在售': '在售',
        '尾盘': '在售',
        '待售': '待售',
        '竟得': '',
        '': ''
    }
    data['OnSaleState'] = status_dict.get(data['OnSaleState'], '')
    return Row(**data)


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select count(MeasuredBuildingArea) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['HousingCount'] = str(df.col.values[0])
    return Row(**data)


def developer(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer'])
    return Row(**data)


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['TotalBuidlingArea'] = str(data['TotalBuidlingArea'])
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
    def getNumber(num):
        c = re.search('-?\d+',num)
        return int(c.group()) if c else 0

    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select ActualFloor as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))

    data['BuildingType'] = check_floor_type(df['col'].apply(getNumber).max()).decode('utf-8')
    return Row(**data)


def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(HouseUseType) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['HouseUseType'] = re.sub(r'^;|;$', '', Meth.jsonDumps(list(set(df.col.values) - set([''])))).replace('结构', '').replace(';;', ';').replace(' ', '')

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
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(LssueDate) as col from PresellInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['LssueDate'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
    return Row(**data)


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(PresalePermitNumber) as col from PresellInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['PresalePermitNumber'] = Meth.jsonDumps(
            list(set(df.col.values) - set([''])))
    return Row(**data)


def houseBuildingCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
    return Row(**data)


def approvalPresaleAmount(data):
    # print(data, inspect.stack()[0][3])
    return data


def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])
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
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select min(LssueDate) as col from PresellInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['EarliestOpeningTime'] = str(df.col.values[0])
    return Row(**data)


def latestDeliversHouseDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def presaleRegistrationManagementDepartment(data):
    # print(data, inspect.stack()[0][3])
    return data


def landLevel(data):
    # print(data, inspect.stack()[0][3])
    return data


def greeningRate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['GreeningRate'] = data['GreeningRate'].replace('%', '')
    return Row(**data)


def floorAreaRatio(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['FloorAreaRatio'] = data['FloorAreaRatio'].replace('%', '')
    return Row(**data)


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
    return Row(**data)


def constructionPermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ConstructionPermitNumber'] = Meth.cleanName(
            data['ConstructionPermitNumber'])
    return Row(**data)


def qualificationNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['QualificationNumber'] = Meth.cleanName(Meth.jsonLoad(
            data['ExtraJson']).get('ExtraQualificationNumber', ''))
    return Row(**data)


def landUsePermit(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPermit(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuildingPermit'] = Meth.cleanName(Meth.jsonLoad(
            data['ExtraJson']).get('ExtraBuildingPermit', ''))
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
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
