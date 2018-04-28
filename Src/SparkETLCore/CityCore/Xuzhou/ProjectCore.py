# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import datetime
import inspect
import sys

import demjson
import pandas as pd
from pyspark.sql import Row

sys.path.append('/home/spark/AwesomeSparkETL/Src')
sys.path.append('/home/spark/AwesomeSparkETL/Src/Script')
sys.path.append('/home/spark/AwesomeSparkETL/Src/SparkETLCore')

from Utils import Meth

ENGINE = Meth.getEngine("spark_test")
METHODS = [
    'approvalPresaleAmount', 'approvalPresaleArea', 'averagePrice',
    'buildingPermit', 'buildingType', 'certificateOfUseOfStateOwnedLand',
    'completionDate', 'constructionPermitNumber', 'decoration', 'developer',
    'districtName', 'earliestOpeningTime', 'earliestStartDate', 'extraJSON',
    'floorArea', 'floorAreaRatio', 'greeningRate', 'houseBuildingCount',
    'houseUseType', 'housingCount', 'landLevel', 'landUse', 'landUsePermit',
    'latestDeliversHouseDate', 'legalPerson', 'legalPersonNumber', 'lssueDate',
    'managementCompany', 'managementFees', 'onSaleState', 'otheRights',
    'parkingSpaceAmount', 'presalePermitNumber',
    'presaleRegistrationManagementDepartment', 'projectAddress',
    'projectApproveData', 'projectBookingData', 'projectName', 'projectType',
    'projectUUID', 'promotionName', 'propertyRightsDescription',
    'qualificationNumber', 'realestateProjectId', 'recordTime', 'regionName',
    'remarks', 'sourceUrl', 'totalBuidlingArea'
]


def recordTime(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    if not data.get("RecordTime"):
        nt = datetime.datetime.now()
        data['RecordTime'] = nt
    data = Row(**data)
    return data


def projectName(data):
    print(inspect.stack()[0][3])
    return data


def promotionName(data):
    print(inspect.stack()[0][3])
    return data


def realestateProjectId(data):
    print(inspect.stack()[0][3])
    return data


def projectUUID(data):
    print(inspect.stack()[0][3])
    return data


def districtName(data):
    print(inspect.stack()[0][3])
    return data


def regionName(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['ExtraRegionName'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraRegionName", ""),
            axis=1
        )
        _ = query['ExtraRegionName'].unique()
        data['RegionName'] = demjson.encode(list(_))
    data = Row(**data)

    return data


def projectAddress(data):
    print(inspect.stack()[0][3])
    return data


def projectType(data):
    print(inspect.stack()[0][3])
    return data


def onSaleState(data):
    print(inspect.stack()[0][3])
    return data


def landUse(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LandUse FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:

        def reshape(val):
            if val:
                val = val.replace('宅', '住宅') \
                    .replace('宅宅', '宅') \
                    .replace('住住', '住') \
                    .replace('、', '/') \
                    .replace('，', ',').strip('/,')
                return val
            return ""

        query['LandUse'] = query.apply(lambda x: reshape(x['LandUse']), axis=1)
        _ = ','.join(list(query['LandUse'][query['LandUse'] != ""]))
        _d = list(set(_.split(',')))
        data['LandUse'] = demjson.encode(_d)
    data = Row(**data)
    return data


def housingCount(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _ = query['MeasuredBuildingArea'].count()
        data['HousingCount'] = str(_)
    data = Row(**data)

    return data


def developer(data):
    print(inspect.stack()[0][3])
    return data


def floorArea(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:

        def reshape(val):
            if val:
                val = demjson.decode(val).get("ExtraLandCertificate", "")
            return val

        query['ExtraLandCertificate'] = query.apply(
            lambda x: reshape(x['ExtraJson']), axis=1)
        query['ExtraFloorArea'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get('ExtraFloorArea'),
            axis=1)
        g = query.groupby(['ExtraLandCertificate'])['ExtraFloorArea'].max()
        g = sum(map(lambda x: float(x), filter(lambda y: y != "", g)))
        data['FloorArea'] = round(g, 2)
    data = Row(**data)

    return data


def totalBuidlingArea(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['MeasuredBuildingArea'] = query.apply(
            lambda x: float(x['MeasuredBuildingArea']) if x['MeasuredBuildingArea'] else 0.0,
            axis=1)
        _ = query['MeasuredBuildingArea'].sum()
        data['TotalBuidlingArea'] = str(_)
    data = Row(**data)

    return data


def buildingType(data):
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.HouseName FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['Floor'] = query.apply(
            lambda x: Meth.getFloor(x['HouseName']), axis=1)
        _ = Meth.bisectCheckFloorType(query['Floor'].max())
        data['BuildingType'] = _
    data = Row(**data)
    return data


def houseUseType(data):
    data = data.asDict()
    print(inspect.stack()[0][3])
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.HouseUseType FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _ = query['HouseUseType'][query['HouseUseType'] != ""].unique()
        data['HouseUseType'] = demjson.encode(_)
    data = Row(**data)

    return data


def propertyRightsDescription(data):
    print(inspect.stack()[0][3])
    return data


def projectApproveData(data):
    print(inspect.stack()[0][3])
    return data


def projectBookingData(data):
    print(inspect.stack()[0][3])
    return data


def lssueDate(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LssueDate FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['LssueDate'] = query.apply(
            lambda x: Meth.cleanName(x['LssueDate']), axis=1)
        _ = query['LssueDate'][query['LssueDate'] != ""] \
            .unique()
        data['LssueDate'] = demjson.encode(list(_))
    data = Row(**data)

    return data


def presalePermitNumber(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.PresalePermitNumber FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['PresalePermitNumber'] = query.apply(
            lambda x: Meth.cleanName(x['PresalePermitNumber'].encode('utf8')),
            axis=1)
        _ = query['PresalePermitNumber'][query['PresalePermitNumber'] != ""] \
            .unique()
        data['PresalePermitNumber'] = demjson.encode(list(_))
    data = Row(**data)
    return data


def houseBuildingCount(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.BuildingName FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        _ = query['BuildingName'][query['BuildingName'] != ""].unique()
        data['HouseBuildingCount'] = len(_)
    data = Row(**data)
    return data


def approvalPresaleAmount(data):
    print(inspect.stack()[0][3])
    return data


def approvalPresaleArea(data):
    print(inspect.stack()[0][3])
    return data


def averagePrice(data):
    print(inspect.stack()[0][3])
    return data


def earliestStartDate(data):
    print(inspect.stack()[0][3])
    return data


def completionDate(data):
    print(inspect.stack()[0][3])
    return data


def earliestOpeningTime(data):
    print(inspect.stack()[0][3])
    return data


def latestDeliversHouseDate(data):
    print(inspect.stack()[0][3])
    return data


def presaleRegistrationManagementDepartment(data):
    print(inspect.stack()[0][3])
    return data


def landLevel(data):
    print(inspect.stack()[0][3])
    return data


def greeningRate(data):
    print(inspect.stack()[0][3])
    return data


def floorAreaRatio(data):
    print(inspect.stack()[0][3])
    return data


def managementFees(data):
    print(inspect.stack()[0][3])
    return data


def managementCompany(data):
    print(inspect.stack()[0][3])
    return data


def otheRights(data):
    print(inspect.stack()[0][3])
    return data


def certificateOfUseOfStateOwnedLand(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['ExtraCertificateOfUseOfStateOwnedLand'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraCertificateOfUseOfStateOwnedLand", ""),
            axis=1
        )
        _ = query['ExtraCertificateOfUseOfStateOwnedLand'][query['ExtraCertificateOfUseOfStateOwnedLand'] != ""] \
            .unique()
        data['CertificateOfUseOfStateOwnedLand'] = demjson.encode(list(_))
    data = Row(**data)

    return data


def constructionPermitNumber(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['ExtraConstructionPermitNumber'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraConstructionPermitNumber", ""),
            axis=1
        )
        _ = query['ExtraConstructionPermitNumber'][
            query['ExtraConstructionPermitNumber'] != ""].unique()
        data['ConstructionPermitNumber'] = demjson.encode(list(_))
    data = Row(**data)

    return data


def qualificationNumber(data):
    print(inspect.stack()[0][3])
    return data


def landUsePermit(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = pd.read_sql(sql, ENGINE)
    if not query.empty:
        query['ExtraLandCertificate'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraLandCertificate", "").replace("、", ""),
            axis=1
        )
        _ = query['ExtraLandCertificate'][
            query['ExtraLandCertificate'] != ""].unique()
        data['LandUsePermit'] = demjson.encode(list(_))
    data = Row(**data)

    return data


def buildingPermit(data):
    print(inspect.stack()[0][3])
    return data


def legalPersonNumber(data):
    print(inspect.stack()[0][3])
    return data


def legalPerson(data):
    print(inspect.stack()[0][3])
    return data


def sourceUrl(data):
    print(inspect.stack()[0][3])
    return data


def decoration(data):
    print(inspect.stack()[0][3])
    return data


def parkingSpaceAmount(data):
    print(inspect.stack()[0][3])
    return data


def remarks(data):
    print(inspect.stack()[0][3])
    return data


def extraJSON(data):
    print(inspect.stack()[0][3])
    data = data.asDict()
    extraj_origin = data.get('ExtraJson')
    if extraj_origin:
        extraj_origin = demjson.decode(extraj_origin)
        extraj = {
            'TotalBuidlingArea': data.get('TotalBuidlingArea', ''),
            'ExtraSaleAddress': extraj_origin['ExtraSaleAddress'],
            'ExtraProjectPoint': extraj_origin['ExtraProjectPoint'],
            'ExtraSoldAmount': extraj_origin['ExtraSoldAmount'],
            'ExtraSoldArea': extraj_origin['ExtraSoldArea'],
            'ExtraUnsoldAmount': extraj_origin['ExtraUnsoldAmount'],
            'ExtraUnsoldArea': extraj_origin['ExtraUnsoldArea'],
        }
        data['ExtraJson'] = demjson.encode(extraj)
    data = Row(**data)
    return data
