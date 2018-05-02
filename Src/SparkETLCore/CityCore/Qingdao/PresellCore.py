# coding=utf-8
from __future__ import division
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')
sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

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
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)


def realEstateProjectID(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,
                     sql = " select RealEstateProjectID as col from ProjectInfoItem where ProjectUUID = '{projectUUID}' and RealEstateProjectID !='' limit 0,1 ".format(
                             projectUUID = data['ProjectUUID']))
    data['RealEstateProjectID'] = str(df.col.values[0]) if not df.empty else ''
    return Row(**data)


def presalePermitNumber(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return Row(**data)


def totalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['TotalBuidlingArea'] = data['TotalBuidlingArea'].replace('㎡', '')
    return Row(**data)


def approvalPresaleAmount(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ApprovalPresaleAmount'] = data['ApprovalPresaleAmount'].replace('套'.decode(), '')
    return Row(**data)


def approvalPresaleArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['ApprovalPresaleArea'] = data['ApprovalPresaleArea'].replace('㎡', '')
    return Row(**data)


def approvalPresaleHouseAmount(data):
    return data


def approvalPresaleHouseArea(data):
    return data


def presaleBuildingAmount(data):
    return data


def constructionFloorCount(data):
    # print(data, inspect.stack()[0][3])
    return data


def builtFloorCount(data):
    # print(data, inspect.stack()[0][3])
    return data


def periodsCount(data):
    # print(data, inspect.stack()[0][3])
    return data


def constructionTotalArea(data):
    return data


def groundArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def underGroundArea(data):
    # print(data, inspect.stack()[0][3])
    return data


def presaleTotalBuidlingArea(data):
    # print(data, inspect.stack()[0][3])
    data = data.asDict()
    data['PresaleTotalBuidlingArea'] = data['PresaleTotalBuidlingArea'].replace('㎡', '')
    return Row(**data)


def contacts(data):
    # print(data, inspect.stack()[0][3])
    return data


def presaleBuildingSupportingAreaInfo(data):
    # print(data, inspect.stack()[0][3])
    return data


def presaleHousingLandIsMortgage(data):
    # print(data, inspect.stack()[0][3])
    return data


def validityDateStartDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def validityDateClosingDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def lssueDate(data):
    # print(data, inspect.stack()[0][3])
    return data


def lssuingAuthority(data):
    # print(data, inspect.stack()[0][3])
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


def presaleHouseCount(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data
