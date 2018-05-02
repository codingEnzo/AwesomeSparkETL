# coding=utf-8
from __future__ import division

import re
import sys
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


def permitUUID(data):
	return data


def recordTime(data):
	return data


def projectName(data):
	data = data.asDict()
	data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	return Row(**data)


def realEstateProjectID(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select ExtraJson as col from ProjectInfoItem where ProjectUUID='{projectUUID}' limit 1 "\
					 .format(projectUUID=data['ProjectUUID']))
	if not df.empty:
		data['RealEstateProjectID'] = Meth.jsonLoad(df.col.values[0]).get('ExtraPropertyID', '')
	return Row(**data)


def presalePermitNumber(data):
	data = data.asDict()
	data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
	return Row(**data)


def totalBuidlingArea(data):
	return data


def approvalPresaleAmount(data):
	return data


def approvalPresaleArea(data):
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


def extraJsondef(data):
	return data
