# coding=utf-8
from __future__ import division

import re
import sys
import pandas as pd
import numpy as np

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
    return data


def projectName(data):
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def promotionName(data):
    data['PromotionName'] = Meth.cleanName(data['PromotionName'])
    return data


def realEstateProjectId(data):
    return data


def projectUUID(data):
    return data


def districtName(data):
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return data


def regionName(data):
    data['RegionName'] = Meth.cleanName(data['RegionName'])
    return data


def projectAddress(data):
    data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
    return data


def projectType(data):
    return data


def onSaleState(data):
    def getNum(key, info):
        v = info.get(key)
        if v:
            c = re.search('\d+', v)
            return int(c.group()) if c else 0
        return 0

    info = Meth.jsonLoad(data['ExtraJson']).get('ExtraProjectSaleInfo')
    if info:
        info = eval(info)
        n1 = getNum('已售非住宅套数', info)
        n2 = getNum('已售住宅套数', info)
        n3 = getNum('总套数', info)
        if n3 != 0:
            data['OnSaleState'] = '售罄' if (n1 + n2) / n3 < 0.1 else '在售'
        else:
            data['OnSaleState'] = ''
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
    c = re.search('-?\d+', data['TotalBuidlingArea'])
    data['TotalBuidlingArea'] = c.group() if c else ''
    return data


def buildingType(data):
    return data


def houseUseType(data):
    arr = data['HouseUseType'].split('@#$')
    if 'null' in arr:
        arr.remove('null')
    if '' in arr:
        arr.remove('')
    data['HouseUseType'] = Meth.jsonDumps(arr)
    return data


def propertyRightsDescription(data):
    return data


def projectApproveData(data):
    return data


def projectBookingdData(data):
    return data


def lssueDate(data):
    data['LssueDate'] = Meth.jsonDumps(data['LssueDate'].split('@#$'))
    return data


def presalePermitNumber(data):
    arr = data['PresalePermitNumber'].split('@#$')
    if '' in arr:
        arr.remove('')
    data['PresalePermitNumber'] = Meth.jsonDumps(arr)


def houseBuildingCount(data):
    data['HouseBuildingCount'] = str(data['HouseBuildingCount'])
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
    data['GreeningRate'] = data['GreeningRate'].replace('%', '')
    return data


def floorAreaRatio(data):
    data['FloorAreaRatio'] = data['FloorAreaRatio'].replace('%', '')
    return data


def managementFees(data):
    return data


def managementCompany(data):
    return data


def otheRights(data):
    return data


def certificateOfUseOfStateOwnedLand(data):
    data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(data['CertificateOfUseOfStateOwnedLand'])
    return data


def constructionPermitNumber(data):
    data['ConstructionPermitNumber'] = Meth.cleanName(data['ConstructionPermitNumber'])
    return data


def qualificationNumber(data):
    data['QualificationNumber'] = Meth.cleanName(Meth.jsonLoad(data['ExtraJson']).get('ExtraQualificationNumber', ''))
    return data


def landUsePermit(data):
    return data


def buildingPermit(data):
    data['BuildingPermit'] = Meth.cleanName(Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingPermit', ''))
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


def extraJson(data):
    return data
