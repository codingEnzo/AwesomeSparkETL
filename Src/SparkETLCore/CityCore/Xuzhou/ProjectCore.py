# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import datetime

import demjson

from SparkETLCore.Utils import Meth

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


def recordTime(spark, data):
    if not data.get("RecordTime"):
        nt = datetime.datetime.now()
        data['RecordTime'] = nt
    return data


def projectName(spark, data):
    return data


def promotionName(spark, data):
    return data


def realestateProjectId(spark, data):
    return data


def projectUUID(spark, data):
    return data


def districtName(spark, data):
    return data


def regionName(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['ExtraRegionName'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraRegionName", ""),
            axis=1
        )
        _ = query['ExtraRegionName'].unique()
        data['RegionName'] = demjson.encode(list(_))

    return data


def projectAddress(spark, data):
    return data


def projectType(spark, data):
    return data


def onSaleState(spark, data):
    return data


def landUse(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LandUse FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
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
    return data


def housingCount(spark, data):
    return data


def developer(spark, data):
    return data


def floorArea(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
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

    return data


def totalBuidlingArea(spark, data):
    return data


def buildingType(spark, data):
    return data


def houseUseType(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.HouseUseType FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        _ = query['HouseUseType'][query['HouseUseType'] != ""].unique()
        data['HouseUseType'] = demjson.encode(_)

    return data


def propertyRightsDescription(spark, data):
    return data


def projectApproveData(spark, data):
    return data


def projectBookingData(spark, data):
    return data


def lssueDate(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LssueDate FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['LssueDate'] = query.apply(
            lambda x: Meth.cleanName(x['LssueDate']), axis=1)
        _ = query['LssueDate'][query['LssueDate'] != ""] \
            .unique()
        data['LssueDate'] = demjson.encode(list(_))

    return data


def presalePermitNumber(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.PresalePermitNumber FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['PresalePermitNumber'] = query.apply(
            lambda x: Meth.cleanName(x['PresalePermitNumber']), axis=1)
        _ = query['PresalePermitNumber'][query['PresalePermitNumber'] != ""] \
            .unique()
        data['PresalePermitNumber'] = demjson.encode(list(_))
    return data


def houseBuildingCount(spark, data):
    # p_uuid = data['ProjectUUID']
    # sql = "SELECT HouseInfoItem.BuildingName FROM HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
    #     p_uuid)
    # query = spark.sql(sql).toPandas()
    # if not query.empty:
    #     _ = query['BuildingName'][query['BuildingName'] != ""].unique()
    #     data['HouseBuildingCount'] = str(len(_))
    return data


def approvalPresaleAmount(spark, data):
    return data


def approvalPresaleArea(spark, data):
    return data


def averagePrice(spark, data):
    return data


def earliestStartDate(spark, data):
    return data


def completionDate(spark, data):
    return data


def earliestOpeningTime(spark, data):
    return data


def latestDeliversHouseDate(spark, data):
    return data


def presaleRegistrationManagementDepartment(spark, data):
    return data


def landLevel(spark, data):
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
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['ExtraCertificateOfUseOfStateOwnedLand'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraCertificateOfUseOfStateOwnedLand", ""),
            axis=1
        )
        _ = query['ExtraCertificateOfUseOfStateOwnedLand'][query['ExtraCertificateOfUseOfStateOwnedLand'] != ""] \
            .unique()
        data['CertificateOfUseOfStateOwnedLand'] = demjson.encode(list(_))

    return data


def constructionPermitNumber(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['ExtraConstructionPermitNumber'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraConstructionPermitNumber", ""),
            axis=1
        )
        _ = query['ExtraConstructionPermitNumber'][
            query['ExtraConstructionPermitNumber'] != ""].unique()
        data['ConstructionPermitNumber'] = demjson.encode(list(_))

    return data


def qualificationNumber(spark, data):
    return data


def landUsePermit(spark, data):
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['ExtraLandCertificate'] = query.apply(
            lambda x: demjson.decode(x['ExtraJson']).get("ExtraLandCertificate", "").replace("、", ""),
            axis=1
        )
        _ = query['ExtraLandCertificate'][
            query['ExtraLandCertificate'] != ""].unique()
        data['LandUsePermit'] = demjson.encode(list(_))

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
    return data


def parkingSpaceAmount(spark, data):
    return data


def remarks(spark, data):
    return data


def extraJSON(spark, data):
    return data
