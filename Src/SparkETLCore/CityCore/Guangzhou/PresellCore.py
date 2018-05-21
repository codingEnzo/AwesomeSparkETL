# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import sys
import inspect
import pandas as pd
import numpy as np
sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from SparkETLCore.Utils import Var, Meth, Config

METHODS = ['approvalPresaleAmount',
           'approvalPresaleArea',
           'approvalPresaleHouseAmount',
           'approvalPresaleHouseArea',
           'approvalPresalePosition',
           'builtFloorCount',
           'constructionFloorCount',
           'constructionTotalArea',
           'contacts',
           'earliestOpeningDate',
           'earliestStartDate',
           'groundArea',
           'houseSpread',
           'landUse',
           'latestDeliversHouseDate',
           'lssueDate',
           'lssuingAuthority',
           'periodsCount',
           'presaleBuildingAmount',
           'presaleBuildingSupportingAreaInfo',
           'presaleHousecount',
           'presaleHousingLandIsMortgage',
           'presalePermitNumber',
           'presalePermittie',
           'presaleRegistrationManagementDepartment',
           'presaleTotalBuidlingArea',
           'projectName',
           'realEstateProjectId',
           'recordTime',
           'remarks',
           'sourceUrl',
           'totalBuidlingArea',
           'underGroundArea',
           'validityDateClosingDate',
           'validityDateDescribe',
           'validityDateStartDate']


def recordTime(data):
    return data


def projectName(data):
    # print(data, inspect.stack()[0][3])

    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])

    data['RealEstateProjectID'] = data['ProjectUUID']
    return data


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])

    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return data


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])

    data['TotalBuidlingArea'] = data['TotalBuidlingArea']
    return data


def approvalPresaleAmount(data):
    # print(data, inspect.stack()[0][3])

    data['ApprovalPresaleAmount'] = data['ApprovalPresaleAmount']
    return data


def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])

    data['ApprovalPresaleArea'] = data['ApprovalPresaleArea']
    return data


def approvalPresaleHouseAmount(data):
    return data


def approvalPresaleHouseArea(data):
    return data


def presaleBuildingAmount(data):
    # print(data, inspect.stack()[0][3])

    data['PresaleBuildingAmount'] = data['PresaleBuildingAmount']
    return data


def constructionFloorCount(data):
    # print(data, inspect.stack()[0][3])

    data['ConstructionFloorCount'] = data['ConstructionFloorCount']
    return data


def builtFloorCount(data):
    # print(data, inspect.stack()[0][3])

    data['BuiltFloorCount'] = data['BuiltFloorCount']
    return data


def periodsCount(data):
    # print(data, inspect.stack()[0][3])

    data['PeriodsCount'] = data['PeriodsCount']
    return data


def constructionTotalArea(data):
    return data


def groundArea(data):
    # print(data, inspect.stack()[0][3])

    data['GroundArea'] = data['GroundArea']
    return data


def underGroundArea(data):
    # print(data, inspect.stack()[0][3])

    data['UnderGroundArea'] = data['UnderGroundArea']
    return data


def presaleTotalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])

    data['PresaleTotalBuidlingArea'] = data['PresaleTotalBuidlingArea']
    return data


def contacts(data):
    # print(data, inspect.stack()[0][3])

    data['Contacts'] = data['Contacts']
    return data


def presaleBuildingSupportingAreaInfo(data):
    # print(data, inspect.stack()[0][3])

    data['PresaleBuildingSupportingAreaInfo'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresaleBuildingSupportingAreaInfo', '')
    return data


def presaleHousingLandIsMortgage(data):
    # print(data, inspect.stack()[0][3])

    data['PresaleHousingLandIsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresaleHousingLandIsMortgage', '')
    return data


def validityDateStartDate(data):
    # print(data, inspect.stack()[0][3])

    data['ValidityDateStartDate'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraValidityDateStartDate', '')
    return data


def validityDateClosingDate(data):
    # print(data, inspect.stack()[0][3])

    data['ValidityDateClosingDate'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraValidityDateClosingDate', '')
    return data


def lssueDate(data):
    # print(data, inspect.stack()[0][3])

    data['LssueDate'] = data['LssueDate']
    return data


def lssuingAuthority(data):
    # print(data, inspect.stack()[0][3])

    data['LssuingAuthority'] = data['LssuingAuthority']
    return data


def presaleRegistrationManagementDepartment(data):
    return data


def validityDateDescribe(data):
    return data


def approvalPresalePosition(data):
    return data


def landUse(data):
    return data


def earliestStartDate(data):
    return data


def latestDeliversHouseDate(data):
    return data


def earliestOpeningDate(data):
    return data


def houseSpread(data):
    return data


def presalePermittie(data):
    return data


def presaleHousecount(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data
