# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')
sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

METHODS = ['approvalPresaleAmount', 'approvalPresaleArea', 'averagePrice', 'buildingPermit', 'buildingType',
           'certificateOfUseOfStateOwnedLand', 'city', 'completionDate', 'constructionPermitNumber', 'decoration',
           'developer', 'districtName', 'earliestOpeningTime', 'earliestStartDate', 'extraJson', 'floorArea',
           'floorAreaRatio', 'greeningRate', 'houseBuildingCount', 'houseUseType', 'housingCount', 'landLevel',
           'landUse', 'landUsePermit', 'latestDeliversHouseDate', 'legalPerson', 'legalPersonNumber', 'lssueDate',
           'managementCompany', 'managementFees', 'onSaleState', 'otheRights', 'parkingSpaceAmount',
           'presalePermitNumber', 'presaleRegistrationManagementDepartment', 'projectAddress', 'projectApproveData',
           'projectBookingData', 'projectID', 'projectName', 'projectType', 'projectUUID', 'promotionName',
           'propertyRightsDescription', 'qualificationNumber', 'realEstateProjectID', 'recordTime', 'regionName',
           'remarks', 'sourceUrl', 'totalBuidlingArea']


def approvalPresaleAmount(data):
    data = data.asDict()
    if not data['ApprovalPresaleAmount']:
        df = pd.read_sql(con = Var.ENGINE,
                         sql = "SELECT ApprovalPresaleAmount AS col FROM PresellInfoItem WHERE ProjectUUID" \
                               " = '{0}' GROUP BY PresalePermitNumber".format(data['ProjectUUID'])).fillna('')
        if not df.empty:
            data['ApprovalPresaleAmount'] = df['col'].apply(lambda x: int(x)).sum()
    return Row(**data)


def approvalPresaleArea(data):
    return data


def averagePrice(data):
    return data


def buildingPermit(data):
    data = data.asDict()
    data['BuildingPermit'] = Meth.cleanName(Meth.jsonLoad(
            data['ExtraJson']).get('ExtraBuildingPermit', ''))
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
        c = re.search('-?\d+', num)
        return int(c.group()) if c else 0

    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select ActualFloor as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    if not df.empty:
        data['BuildingType'] = check_floor_type(df['col'].apply(getNumber).max()).decode('utf-8')
    return Row(**data)


def certificateOfUseOfStateOwnedLand(data):
    data = data.asDict()
    data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(
            data['CertificateOfUseOfStateOwnedLand'])
    return Row(**data)


def city(data):
    return data


def completionDate(data):
    return data


def constructionPermitNumber(data):
    data = data.asDict()
    data['ConstructionPermitNumber'] = Meth.cleanName(
            data['ConstructionPermitNumber'])
    return Row(**data)


def decoration(data):
    return data


def developer(data):
    data = data.asDict()
    data['Developer'] = Meth.cleanName(data['Developer'])
    return Row(**data)


def districtName(data):
    data = data.asDict()
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return Row(**data)


def earliestOpeningTime(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select min(LssueDate) as col from PresellInfoItem where ProjectUUID='{projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['EarliestOpeningTime'] = str(df.col.values[0]) if not df.empty else ''
    return Row(**data)


def earliestStartDate(data):
    return data


def extraJson(data):
    return data


def floorArea(data):
    return data


def floorAreaRatio(data):
    data = data.asDict()
    c = re.search('-?\d+', data['FloorAreaRatio'])
    data['FloorAreaRatio'] = c.group() if c else ''
    return Row(**data)


def greeningRate(data):
    data = data.asDict()
    c = re.search('-?\d+', data['GreeningRate'])
    data['GreeningRate'] = c.group() if c else ''
    return Row(**data)


def houseBuildingCount(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{"
                           "projectUUID}'".format(projectUUID = data['ProjectUUID']))
    if not df.empty:
        data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
    return Row(**data)


def houseUseType(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(HouseUseType) as col from HouseInfoItem where ProjectUUID='{"
                           "projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['HouseUseType'] = re.sub(r'^;|;$', '', Meth.jsonDumps(list(set(df.col.values) - set([''])))) \
        .replace('结构'.decode('utf-8'), '').replace(';;', ';').replace(' ', '')

    return Row(**data)


def housingCount(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select count(MeasuredBuildingArea) as col from HouseInfoItem where ProjectUUID='{"
                           "projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    data['HousingCount'] = str(df.col.values[0])
    return Row(**data)


def landLevel(data):
    return data


def landUse(data):
    return data


def landUsePermit(data):
    return data


def latestDeliversHouseDate(data):
    return data


def legalPerson(data):
    return data


def legalPersonNumber(data):
    return data


def lssueDate(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(LssueDate) as col from PresellInfoItem where ProjectUUID='{"
                           "projectUUID}'".format(
                             projectUUID = data['ProjectUUID']))
    if not df.empty:
        data['LssueDate'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
    return Row(**data)


def managementCompany(data):
    return data


def managementFees(data):
    return data


def onSaleState(data):
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


def otheRights(data):
    return data


def parkingSpaceAmount(data):
    return data


def presalePermitNumber(data):
    data = data.asDict()
    df = pd.read_sql(con = Var.ENGINE,
                     sql = "select distinct(PresalePermitNumber) as col from PresellInfoItem "
                           "where ProjectUUID='{0}'".format(data['ProjectUUID']))
    if not df.empty:
        data['PresalePermitNumber'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
    return Row(**data)


def presaleRegistrationManagementDepartment(data):
    return data


def projectAddress(data):
    data = data.asDict()
    data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
    return Row(**data)


def projectApproveData(data):
    return data


def projectBookingData(data):
    return data


def projectID(data):
    data = data.asDict()
    if not data['ProjectID']:
        df = pd.read_sql(con = Var.ENGINE,
                         sql = " SELECT ProjectID as col FROM ProjectInfoItem WHERE ProjectUUID = '{projectUUID}'  AND ProjectID !='' LIMIT 1 ".format(
                                 projectUUID = data['ProjectUUID']))
        data['ProjectID'] = df.col.values[0] if not df.empty else ''
    return Row(**data)


def projectName(data):
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def projectType(data):
    return data


def projectUUID(data):
    return data


def promotionName(data):
    data = data.asDict()
    data['PromotionName'] = Meth.cleanName(data['PromotionName'])
    return Row(**data)


def propertyRightsDescription(data):
    return data


def qualificationNumber(data):
    data = data.asDict()
    data['QualificationNumber'] = Meth.cleanName(Meth.jsonLoad(
            data['ExtraJson']).get('ExtraQualificationNumber', ''))
    return Row(**data)


def realEstateProjectID(data):
    data = data.asDict()
    data['RealEstateProjectID'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraPropertyID', ''))
    return Row(**data)


def recordTime(data):
    return data


def regionName(data):
    data = data.asDict()
    data['RegionName'] = Meth.cleanName(data['RegionName'])
    return Row(**data)


def remarks(data):
    return data


def sourceUrl(data):
    return data


def totalBuidlingArea(data):
    data = data.asDict()
    c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['TotalBuidlingArea'])
    if c:
        data['TotalBuidlingArea'] = c.group()
    else:
        df = pd.read_sql(con = Var.ENGINE,
                         sql = "SELECT TotalBuidlingArea AS col FROM PresellInfoItem WHERE " \
                               "ProjectUUID = '{0}' GROUP BY PresalePermitNumber".format(data['ProjectUUID']))
        if not df.empty:
            data['TotalBuidlingArea'] = df['col'].apply(lambda x: float(x)).sum()
    return Row(**data)
