import datetime
import sys

import pandas as pd
from pyspark.sql import Row

from Utils.Meth import cleanName, dateFormatter, getEngine

sys.path.append('/home/lin/AwesomeSparkETL/Src/SparkETLCore')
ENGINE = getEngine("spark_test")


def recordTime(data):
    if not data.get('RecordTime'):
        nt = datetime.datetime.now()
        _row = data.asDict()
        _row['RecordTime'] = nt
        data = Row(**_row)
    return data


def projectName(data):
    return data


def realEstateProjectID(data):
    _row = data.asDict()
    _row['realEstateProjectID'] = _row.get('ProjectUUID', '')
    data = Row(**_row)
    return data


def presalePermitNumber(data):
    _pnum = data.get('PresalePermitNumber', '')
    _row = data.asDict()
    _pnum = cleanName(_pnum)
    _row['PresalePermitNumber'] = _pnum
    data = Row(**_row)
    return data


def totalBuidlingArea(data):
    return data


def approvalPresaleAmount(data):
    return data


def approvalPresaleArea(data):
    return data


def approvalPresaleHouseAmount(data):
    data = data.asDict()
    permit_num = data.get('PresalePermitNum', "")
    sql = """
    SELECT * FROM (SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "住宅"
    UNION ALL
     SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "商住"
    UNION ALL
     SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "阁楼"
    ) t
    """.format(permit_num)
    q = pd.read_sql(sql, ENGINE)
    house_amount = 0
    if not q.empty:
        house_amount = q['MeasuredBuildingArea'].count()
    data['presaleTotalBuildingArea'] = house_amount
    data = Row(**data)
    return data


def approvalPresaleHouseArea(data):
    data = data.asDict()
    permit_num = data.get('PresalePermitNum', "")
    sql = """
    SELECT * FROM (SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "住宅"
    UNION ALL
     SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "商住"
    UNION ALL
     SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
     WHERE PresellInfoItem.PresalePermitNumber = "20140736" AND HouseInfoItem.HouseUseType = "阁楼"
    ) t
    """.format(permit_num)
    q = pd.read_sql(sql, ENGINE)
    house_amount = 0
    if not q.empty:
        house_amount = q['MeasuredBuildingArea'].sum()
    data['presaleTotalBuildingArea'] = house_amount
    data = Row(**data)
    return data


def presaleBuildingAmount(data):
    _row = data.asDict()
    _pba = data.get('PresaleBuildingAmount', '')
    _pba = str(len(set(_pba.replace('、', ',').split(','))))
    _row['PresaleBuildingAmount'] = _pba
    data = Row(**_row)
    return data


def constructionFloorCount(data):
    return data


def builtFloorCount(data):
    data = data.asDict()
    permit_num = data.get('PresalePermitNum', "")
    sql = """
    SELECT HouseInfoItem.Floor FROM HouseInfoItem
    JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
    WHERE PresellInfoItem.PresalePermitNumber = "{}"
    """.format(permit_num)
    q = pd.read_sql(sql, ENGINE)
    b_count = 0
    if not q.empty:
        b_count = q['Floor'].unique().count()
    data['BuiltFloorCount'] = b_count
    data = Row(**data)
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
    data = data.asDict()
    permit_num = data.get('PresalePermitNum', "")
    sql = """
    SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
    JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
    WHERE PresellInfoItem.PresalePermitNumber = "{}"
    """.format(permit_num)
    q = pd.read_sql(sql, ENGINE)
    tt_b_area = '0'
    if not q.empty:
        tt_b_area = '{}'.format(q['MeasuredBuildingArea'].sum())
    data['presaleTotalBuildingArea'] = tt_b_area
    data = Row(**data)
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
    ds = data['LssueDate']
    _row = data.asDict()
    _row['LssueDate'] = dateFormatter(ds)
    data = Row(**_row)
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
    _use = data['LandUse'].replace('、', '/').replace('，', ',').split(',')
    _row = data.asDict()
    _row['LandUse'] = _use
    data = Row(**_row)
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
    data = data.asDict()
    permit_num = data.get('PresalePermitNum', "")
    sql = """
    SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
    JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
    WHERE PresellInfoItem.PresalePermitNumber = "{}"
    """.format(permit_num)
    q = pd.read_sql(sql, ENGINE)
    h_count = 0
    if not q.empty:
        h_count = q['MeasuredBuildingArea'].count()
    data['presaleHouseCount'] = h_count
    data = Row(**data)
    return data


def remarks(data):
    _extraj = data.get('ExtraJson')
    if _extraj:
        _row = data.asDict()
        _row['rmarks'] = _extraj.get('ExtraPresellUUID', '')
        data = Row(**_row)
    return data


def sourceUrl(data):
    return data
