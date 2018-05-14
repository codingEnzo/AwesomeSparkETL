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
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectId(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['RealEstateProjectID'] = data['ProjectUUID']
    return Row(**data)


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return Row(**data)


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['TotalBuidlingArea'] = data['TotalBuidlingArea']
    return Row(**data)


def approvalPresaleAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ApprovalPresaleAmount'] = data['ApprovalPresaleAmount']
    return Row(**data)


def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ApprovalPresaleArea'] = data['ApprovalPresaleArea']
    return Row(**data)


def approvalPresaleHouseAmount(data):
    return data


def approvalPresaleHouseArea(data):
    return data


def presaleBuildingAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresaleBuildingAmount'] = data['PresaleBuildingAmount']
    return Row(**data)


def constructionFloorCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ConstructionFloorCount'] = data['ConstructionFloorCount']
    return Row(**data)


def builtFloorCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['BuiltFloorCount'] = data['BuiltFloorCount']
    return Row(**data)


def periodsCount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PeriodsCount'] = data['PeriodsCount']
    return Row(**data)


def constructionTotalArea(data):
    return data


def groundArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['GroundArea'] = data['GroundArea']
    return Row(**data)


def underGroundArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['UnderGroundArea'] = data['UnderGroundArea']
    return Row(**data)


def presaleTotalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresaleTotalBuidlingArea'] = data['PresaleTotalBuidlingArea']
    return Row(**data)


def contacts(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['Contacts'] = data['Contacts']
    return Row(**data)


def presaleBuildingSupportingAreaInfo(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresaleBuildingSupportingAreaInfo'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresaleBuildingSupportingAreaInfo', '')
    return Row(**data)


def presaleHousingLandIsMortgage(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresaleHousingLandIsMortgage'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraPresaleHousingLandIsMortgage', '')
    return Row(**data)


def validityDateStartDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ValidityDateStartDate'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraValidityDateStartDate', '')
    return Row(**data)


def validityDateClosingDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ValidityDateClosingDate'] = Meth.jsonLoad(
        data['ExtraJson']).get('ExtraValidityDateClosingDate', '')
    return Row(**data)


def lssueDate(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['LssueDate'] = data['LssueDate']
    return Row(**data)


def lssuingAuthority(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['LssuingAuthority'] = data['LssuingAuthority']
    return Row(**data)


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
