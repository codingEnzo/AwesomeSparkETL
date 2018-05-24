# coding=utf-8
from __future__ import division
import sys
import inspect
import os
# sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from SparkETLCore.Utils import Meth, Config, Var

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
           'totalBuidlingArea',
           'projectID']


def recordTime(data):

    return data


def projectID(data):

    return data


def projectName(data):

    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def promotionName(data):

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

    data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
    return data


def projectType(data):

    return data


def onSaleState(data):
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
    data['TotalBuidlingArea'] = Meth.cleanUnit(data['TotalBuidlingArea'])
    return data


def buildingType(data):
    return data


def houseUseType(data):
    if data['BuildingExtraJson']:
        cache = set(map(lambda x: Meth.cleanName(Meth.jsonLoad(x).get(
            'ExtraBuildingUsage', '')) if x else '', data['BuildingExtraJson'].split('@#$')))
        if len(cache) > 0:
            data['HouseUseType'] = Meth.jsonDumps(list(cache - set([''])))
    return data


def propertyRightsDescription(data):

    return data


def projectApproveData(data):

    return data


def projectBookingdData(data):

    return data


def lssueDate(data):

    return data


def presalePermitNumber(data):
    if data['PresalePermitNumber']:
        data['PresalePermitNumber'] = Meth.cleanName(
            data['PresalePermitNumber'])
        data['PresalePermitNumber'] = Meth.jsonDumps(
            list(set(data['PresalePermitNumber'].split('@#$'))))
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

    if data['BuildingExtraJson']:
        cache = set(map(lambda x: Meth.jsonLoad(x).get('ExtraBuildingOpenDate', '')
                        if x else '', data['BuildingExtraJson'].split('@#$'))) - set([''])
        if len(cache) > 0:
            data['EarliestOpeningTime'] = min(cache).replace('/', '-')

    return data


def latestDeliversHouseDate(data):

    if data['BuildingExtraJson']:
        cache = set(map(lambda x: Meth.jsonLoad(x).get(
            'ExtraBuildingDeliverDate', '') if x else '', data['BuildingExtraJson'].split('@#$')))

        if cache:
            data['LatestDeliversHouseDate'] = max(cache).replace('/', '-')
    return data


def presaleRegistrationManagementDepartment(data):

    return data


def landLevel(data):

    return data


def greeningRate(data):
    data['GreeningRate'] = Meth.cleanUnit(data['GreeningRate'])

    return data


def floorAreaRatio(data):

    data['FloorAreaRatio'] = Meth.cleanUnit(data['FloorAreaRatio'])

    return data


def managementFees(data):

    return data


def managementCompany(data):

    data['ManagementCompany'] = Meth.cleanName(data['ManagementCompany'])

    return data


def otheRights(data):

    return data


def certificateOfUseOfStateOwnedLand(data):

    if data['BuildingExtraJson']:
        cache = set(map(lambda x: Meth.cleanName(Meth.jsonLoad(x).get(
            'ExtraBuildingAreaCode', '')) if x else '', data['BuildingExtraJson'].split('@#$')))
        if len(cache) > 0:
            data['CertificateOfUseOfStateOwnedLand'] = Meth.jsonDumps(
                (list(cache - set(['']))))
    return data


def constructionPermitNumber(data):

    return data


def qualificationNumber(data):

    return data


def landUsePermit(data):

    return data


def buildingPermit(data):

    if data['BuildingExtraJson']:
        cache = set(map(lambda x: Meth.cleanName(Meth.jsonLoad(x).get(
            'ExtraBuildingPlanCode', '')) if x else '', data['BuildingExtraJson'].split('@#$')))
        if len(cache) > 0:
            data['BuildingPermit'] = Meth.jsonDumps((list(cache - set(['']))))
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
