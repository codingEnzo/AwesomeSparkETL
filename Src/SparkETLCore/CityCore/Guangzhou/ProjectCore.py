# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import sys
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import SparkSession
from Utils import Var, Meth, Config

spark = SparkSession\
        .builder\
        .appName('guangzhou')\
        .getOrCreate()

METHODS = ['approvalPresaleAmount', 'approvalPresaleArea', 'averagePrice', 'buildingPermit', 'buildingType', 'certificateOfUseOfStateOwnedLand', 'completionDate', 'constructionPermitNumber', 'decoration', 'developer', 'districtName', 'earliestOpeningTime', 'earliestStartDate', 'extraJson', 'floorArea',
           'floorAreaRatio', 'greeningRate', 'houseBuildingCount', 'houseUseType', 'housingCount', 'landLevel', 'landUse', 'landUsePermit', 'latestDeliversHouseDate', 'legalPerson', 'legalPersonNumber', 'lssueDate',
           'managementCompany', 'managementFees', 'onSaleState', 'otheRights', 'parkingSpaceAmount', 'presalePermitNumber', 'presaleRegistrationManagementDepartment', 'projectAddress', 'projectApproveData', 'projectBookingdData', 'projectName',
           'projectType', 'projectUUID', 'promotionName', 'propertyRightsDescription', 'qualificationNumber', 'realEstateProjectId', 'recordTime', 'regionName', 'remarks', 'sourceUrl', 'totalBuidlingArea']


def recordTime(data):
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

    data['RealEstateProjectID'] = str(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectID', ''))
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

    data['ProjectAddress'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraProjectAddress', '')
    return data


def projectType(data):
    # print(data, inspect.stack()[0][3])
    return data


def onSaleState(data):
    # print(data, inspect.stack()[0][3])

    unsoldNum = int(Meth.jsonLoad(data['ExtraJson']).get(
        'ExtraTotalUnsoldAmount', '0'))
    presaleNum = int(data.get('ApprovalPresaleAmount', '0'))
    if not presaleNum:
        data['OnSaleState'] = '售馨'
    else:
        data['OnSaleState'] = '售馨' if (unsoldNum / presaleNum) < 0.1 else '在售'
    return data


def landUse(data):
    # print(data, inspect.stack()[0][3])
    return data


def housingCount(data):
    # print(data, inspect.stack()[0][3])
    return data


def developer(data):
    # print(data, inspect.stack()[0][3])
    data['Developer'] = Meth.cleanName(data['Developer'])
    return data


def floorArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data['TotalBuidlingArea'] = str(data['TotalBuidlingArea'])
    return data


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
    # print(data, inspect.stack()[0][3])
    data['BuildingType'] = ''
    return data


def houseUseType(data):
    # print(data, inspect.stack()[0][3])
    data['HouseUseType'] = Meth.jsonDumps(data['HouseUseType'].split('@#$'))
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
    data['LssueDate'] = Meth.jsonDumps(data['LssueDate'].split('@#$'))
    return data


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data['PresalePermitNumber'] = Meth.jsonDumps(data['PresalePermitNumber'].split('@#$'))
    return data


def houseBuildingCount(data):
    # print(data, inspect.stack()[0][3])
    return data


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
    return data


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

    data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(
        data['CertificateOfUseOfStateOwnedLand'])
    return data


def constructionPermitNumber(data):
    # print(data, inspect.stack()[0][3])

    data['ConstructionPermitNumber'] = Meth.cleanName(
        data['ConstructionPermitNumber'])
    return data


def qualificationNumber(data):
    # print(data, inspect.stack()[0][3])

    data['QualificationNumber'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraQualificationNumber', ''))
    return data


def landUsePermit(data):
    # print(data, inspect.stack()[0][3])
    return data


def buildingPermit(data):
    # print(data, inspect.stack()[0][3])

    data['BuildingPermit'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraBuildingPermit', ''))
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

    data['ParkingSpaceAmount'] = Meth.cleanName(Meth.jsonLoad(
        data['ExtraJson']).get('ExtraParkingTotalSoldAmount', ''))
    return data


def remarks(data):
    # print(data, inspect.stack()[0][3])
    return data


def extraJson(data):
    # print(data, inspect.stack()[0][3])
    return data
