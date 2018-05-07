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
	data = data.asDict()
	data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	return Row(**data)


def promotionName(data):
	data = data.asDict()
	data['PromotionName'] = Meth.cleanName(data['PromotionName'])
	return Row(**data)


def realEstateProjectId(data):
	return data


def projectUUID(data):
	return data


def districtName(data):
	data = data.asDict()
	data['DistrictName'] = Meth.cleanName(data['DistrictName'])
	return Row(**data)


def regionName(data):
	data = data.asDict()
	data['RegionName'] = Meth.cleanName(data['RegionName'])
	return Row(**data)


def projectAddress(data):
	data = data.asDict()
	data['ProjectAddress'] = Meth.cleanName(data['ProjectAddress'])
	return Row(**data)


def projectType(data):
	return data


def onSaleState(data):
	def getNum(key,info):
		v = info.get(key)
		if v:
			c = re.search('\d+',v)
			return int(c.group()) if c else 0
		return 0

	data = data.asDict()
	info = Meth.jsonLoad(data['ExtraJson']).get('ExtraProjectSaleInfo')
	if info:
		info = eval(info)
		n1 = getNum('已售非住宅套数',info)
		n2 = getNum('已售住宅套数',info)
		n3 = getNum('总套数',info)
		if n3 != 0:
			data['OnSaleState'] = '售罄'.decode('utf-8') if (n1 + n2) / n3 < 0.1 else '在售'.decode('utf-8')
		else:
			data['OnSaleState'] = ''
	return Row(**data)


def landUse(data):
	return data


def housingCount(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select count(MeasuredBuildingArea) as col from HouseInfoItem where ProjectUUID='{0}'" \
					 .format(data['ProjectUUID']))
	data['HousingCount'] = str(df.col.values[0])
	return Row(**data)


def developer(data):
	data = data.asDict()
	data['Developer'] = Meth.cleanName(data['Developer'])
	return Row(**data)


def floorArea(data):
	return data


def totalBuidlingArea(data):
	data = data.asDict()
	c = re.search('-?\d+',data['TotalBuidlingArea'])
	data['TotalBuidlingArea'] = c.group() if c else ''
	return Row(**data)


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

	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select max(ActualFloor) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['BuildingType'] = check_floor_type(df.col.values[0]).decode('utf-8')
	return Row(**data)


def houseUseType(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(HouseUseType) as col from HouseInfoItem where ProjectUUID='{projectUUID}'" \
					 .format(projectUUID=data['ProjectUUID']))
	data['HouseUseType'] = re.sub(r'^;|;$', '', Meth.jsonDumps(list(set(df.col.values) - set([''])))) \
		.replace('结构'.decode('utf-8'), '').replace(';;', ';').replace(' ', '')
	return Row(**data)


def propertyRightsDescription(data):
	return data


def projectApproveData(data):
	return data


def projectBookingdData(data):
	return data


def lssueDate(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(LssueDate) as col from PresellInfoItem where ProjectUUID='{0}'" \
					 .format(data['ProjectUUID']))
	data['LssueDate'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
	return Row(**data)


def presalePermitNumber(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(PresalePermitNumber) as col from PresellInfoItem where ProjectUUID='{0}'" \
					 .format(data['ProjectUUID']))
	data['PresalePermitNumber'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
	return Row(**data)


def houseBuildingCount(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(BuildingUUID) as col from HouseInfoItem where ProjectUUID='{projectUUID}'"\
					 .format(projectUUID=data['ProjectUUID']))
	data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
	return Row(**data)


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
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select min(LssueDate) as col from PresellInfoItem where ProjectUUID='{projectUUID}'" \
					 .format(projectUUID=data['ProjectUUID']))
	data['EarliestOpeningTime'] = str(df.col.values[0]) if not df.empty else ''
	return Row(**data)


def latestDeliversHouseDate(data):
	return data


def presaleRegistrationManagementDepartment(data):
	return data


def landLevel(data):
	return data


def greeningRate(data):
	data = data.asDict()
	data['GreeningRate'] = data['GreeningRate'].replace('%', '')
	return Row(**data)


def floorAreaRatio(data):
	data = data.asDict()
	data['FloorAreaRatio'] = data['FloorAreaRatio'].replace('%', '')
	return Row(**data)


def managementFees(data):
	return data


def managementCompany(data):
	return data


def otheRights(data):
	return data


def certificateOfUseOfStateOwnedLand(data):
	data = data.asDict()
	data['CertificateOfUseOfStateOwnedLand'] = Meth.cleanName(
		data['CertificateOfUseOfStateOwnedLand'])
	return Row(**data)


def constructionPermitNumber(data):
	data = data.asDict()
	data['ConstructionPermitNumber'] = Meth.cleanName(
		data['ConstructionPermitNumber'])
	return Row(**data)


def qualificationNumber(data):
	data = data.asDict()
	data['QualificationNumber'] = Meth.cleanName(Meth.jsonLoad(
		data['ExtraJson']).get('ExtraQualificationNumber', ''))
	return Row(**data)


def landUsePermit(data):
	return data


def buildingPermit(data):
	data = data.asDict()
	data['BuildingPermit'] = Meth.cleanName(Meth.jsonLoad(
		data['ExtraJson']).get('ExtraBuildingPermit', ''))
	return Row(**data)


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
