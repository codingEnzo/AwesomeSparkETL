# coding=utf-8
from __future__ import division
from __future__ import unicode_literals

from SparkETLCore.Utils import Meth

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
           'projectID',
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
    return data


def projectID(data):
    return data


def realEstateProjectId(data):
    return data


def projectUUID(data):
    return data


def districtName(data):
    data['DistrictName'] = Meth.cleanName(data['DistrictName'])
    return data


def regionName(data):
    return data


def projectAddress(data):
    return data


def projectType(data):
    return data


def onSaleState(data):
    def getOnSaleState(state):
        status_dict = {
            '即将开盘': '待售',
            '即将开盘,在售': '',
            '即将开盘,在售,售完': '',
            '售完': '售罄',
            '在售': '在售',
        }
        return status_dict.get(state, '')

    data['OnSaleState'] = getOnSaleState(data['OnSaleState'])
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
    data['TotalBuidlingArea'] = data['TotalBuidlingArea'].replace('㎡', '')
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
    return data


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
    return data


def qualificationNumber(data):
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
    return data


def parkingSpaceAmount(data):
    return data


def remarks(data):
    return data


def extraJson(data):
    return data
