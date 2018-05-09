# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import datetime

import demjson
from pyspark.sql import SparkSession

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


def recordTime(data):
    if not data.get("RecordTime"):
        nt = datetime.datetime.now()
        data['RecordTime'] = nt
    return data


def projectName(data):
    return data


def promotionName(data):
    return data


def realestateProjectId(data):
    return data


def projectUUID(data):
    return data


def districtName(data):
    return data


def regionName(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM global_temp.PresellInfoItem " \
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


def projectAddress(data):
    return data


def projectType(data):
    return data


def onSaleState(data):
    return data


def landUse(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LandUse FROM global_temp.PresellInfoItem " \
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


def housingCount(data):
    return data


def developer(data):
    return data


def floorArea(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM global_temp.PresellInfoItem " \
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


def totalBuidlingArea(data):
    return data


def buildingType(data):
    return data


def houseUseType(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT HouseInfoItem.HouseUseType FROM global_temp.HouseInfoItem WHERE HouseInfoItem.ProjectUUID = '{}'".format(
        p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        _ = query['HouseUseType'][query['HouseUseType'] != ""].unique()
        data['HouseUseType'] = demjson.encode(_)

    return data


def propertyRightsDescription(data):
    return data


def projectApproveData(data):
    return data


def projectBookingData(data):
    return data


def lssueDate(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.LssueDate FROM global_temp.PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['LssueDate'] = query.apply(
            lambda x: Meth.cleanName(x['LssueDate']), axis=1)
        _ = query['LssueDate'][query['LssueDate'] != ""] \
            .unique()
        data['LssueDate'] = demjson.encode(list(_))

    return data


def presalePermitNumber(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.PresalePermitNumber FROM global_temp.PresellInfoItem " \
          "WHERE PresellInfoItem.ProjectUUID = '{}'".format(p_uuid)
    query = spark.sql(sql).toPandas()
    if not query.empty:
        query['PresalePermitNumber'] = query.apply(
            lambda x: Meth.cleanName(x['PresalePermitNumber']), axis=1)
        _ = query['PresalePermitNumber'][query['PresalePermitNumber'] != ""] \
            .unique()
        data['PresalePermitNumber'] = demjson.encode(list(_))
    return data


def houseBuildingCount(data):
    return data


def approvalPresaleAmount(data):
    return data


def approvalPresaleArea(data):
    return data


def averagePrice(data):
    return data


def earliestStartDate(data):
    return data


def completionDate(data):
    return data


def earliestOpeningTime(data):
    return data


def latestDeliversHouseDate(data):
    return data


def presaleRegistrationManagementDepartment(data):
    return data


def landLevel(data):
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
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM global_temp.PresellInfoItem " \
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


def constructionPermitNumber(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM global_temp.PresellInfoItem " \
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


def qualificationNumber(data):
    return data


def landUsePermit(data):
    spark = SparkSession.builder.appName('xuzhou').getOrCreate()
    p_uuid = data['ProjectUUID']
    sql = "SELECT PresellInfoItem.ExtraJson FROM global_temp.PresellInfoItem " \
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


def buildingPermit(data):
    return data


def legalPersonNumber(data):
    return data


def legalPerson(data):
    return data


def sourceUrl(data):
    return data


def decoration(data):
    return data


def parkingSpaceAmount(data):
    return data


def remarks(data):
    return data


def extraJSON(data):
    return data
