# coding=utf-8
from __future__ import division
from __future__ import unicode_literals
import pandas as pd
import numpy as np
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
           'presaleHouseCount',
           'presaleHousingLandIsMortgage',
           'presalePermitNumber',
           'presalePermitTie',
           'presaleRegistrationManagementDepartment',
           'presaleTotalBuidlingArea',
           'projectName',
           'realEstateProjectID',
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
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return data


def realEstateProjectID(data):
    return data


def presalePermitNumber(data):
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return data


def totalBuidlingArea(data):
    return data


def approvalPresaleAmount(data):
    data['ApprovalPresaleAmount'] = data['ApprovalPresaleAmount'].replace('套', '')
    return data


def approvalPresaleArea(data):
    data['ApprovalPresaleArea'] = data['ApprovalPresaleArea'].replace('㎡', '')
    return data


def approvalPresaleHouseAmount(data):
    return data


def approvalPresaleHouseArea(data):
    return data


def presaleBuildingAmount(data):
    return data


def constructionFloorCount(data):
    return data


def builtFloorCount(data):
    return data


def periodsCount(data):
    return data


def constructionTotalArea(data):
    return data


def groundArea(data):
    return data


def underGroundArea(data):
    return data


def presaleTotalBuidlingArea(data):
    return data


def contacts(data):
    return data


def presaleBuildingSupportingAreaInfo(data):
    return data


def presaleHousingLandIsMortgage(data):
    return data


def validityDateStartDate(data):
    return data


def validityDateClosingDate(data):
    return data


def lssueDate(data):
    return data


def lssuingAuthority(data):
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


def presalePermitTie(data):
    return data


def presaleHouseCount(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data
