# coding=utf-8
from __future__ import division
import sys

reload(sys)
sys.setdefaultencoding("utf-8")
import inspect
import pandas as pd
from pyspark.sql import Row

sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from Utils import Var, Meth, Config

METHODS = ['approvalPresaleAmount',
		   'approvalPresaleArea',
		   'averagePrice',
		   'buildingPermit',
		   'buildingType',
		   'certificateOfUseOfStateOwnedLand',
		   'completionDate',
		   'constructionPermitNumber',
		   'city',
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

def city(data):
	data = data.asDict()
	data['City'] = '青岛'.decode('utf-8')
	return Row(**data)
def recordTime(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['ProjectName'] = Meth.cleanName(data['ProjectName'])

	return Row(**data)


def promotionName(data):
	# print(data, inspect.stack()[0][3])
	return data


def realEstateProjectId(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectUUID(data):
	# print(data, inspect.stack()[0][3])
	return data


def districtName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['DistrictName'] = Meth.cleanName(str(data['DistrictName']))
	return Row(**data)


def regionName(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectAddress(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectType(data):
	# print(data, inspect.stack()[0][3])
	return data


def onSaleState(data):
	# print(data, inspect.stack()[0][3])

	def getOnSaleState(state):
		status_dict = {
			u'即将开盘': '待售'.decode('utf-8'),
			u'即将开盘,在售': '',
			u'即将开盘,在售,售完': '',
			u'售完': '售罄'.decode('utf-8'),
			u'在售': '在售'.decode('utf-8'),
		}
		return status_dict.get(state, '')

	data = data.asDict()
	data['OnSaleState'] = getOnSaleState(data['OnSaleState'])
	return Row(**data)


def landUse(data):
	# print(data, inspect.stack()[0][3])
	return data


def housingCount(data):
	# print(data, inspect.stack()[0][3])
	return data


def developer(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['Developer'] = Meth.cleanName(data['Developer'])
	return Row(**data)


def floorArea(data):
	# print(data, inspect.stack()[0][3])
	return data


def totalBuidlingArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['TotalBuidlingArea'] = data['TotalBuidlingArea'].replace('㎡', '')
	return Row(**data)


def buildingType(data):
	def check_floor_type(floorname):
		if floorname <= 3:
			return '低层(1-3)'.decode()
		elif floorname <= 6:
			return '多层(4-6)'.decode()
		elif floorname <= 11:
			return '小高层(7-11)'.decode()
		elif floorname <= 18:
			return '中高层(12-18)'.decode()
		elif floorname <= 32:
			return '高层(19-32)'.decode()
		elif floorname >= 33:
			return '超高层(33)'.decode()
		else:
			return ''

	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select max(ActualFloor) as col from HouseInfoItem where ProjectUUID='{projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['BuildingType'] = check_floor_type(df.col.values[0]).decode('utf-8')
	# print(data['BuildingType'])
	return Row(**data)


def houseUseType(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(HouseUseType) as col from HouseInfoItem where ProjectUUID='{"
						 "projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['HouseUseType'] = Meth.jsonDumps(list(set(df.col.values) - set([''])))
	return Row(**data)


def propertyRightsDescription(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectApproveData(data):
	# print(data, inspect.stack()[0][3])
	return data


def projectBookingdData(data):
	# print(data, inspect.stack()[0][3])
	return data


def lssueDate(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(LssueDate) as col from PresellInfoItem where ProjectUUID='{"
						 "projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['LssueDate'] = ';'.join(set(df.col.values) - set(['']))
	return Row(**data)


def presalePermitNumber(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(PresalePermitNumber) as col from PresellInfoItem where ProjectUUID='{"
						 "projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['PresalePermitNumber'] = Meth.jsonDumps(
		list(set(df.col.values) - set([''])))
	return Row(**data)


def houseBuildingCount(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select distinct(BuildingName) as col from HouseInfoItem where ProjectUUID='{"
						 "projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))
	data['HouseBuildingCount'] = str(len(list(set(df.col.values) - set(['']))))
	return Row(**data)


def approvalPresaleAmount(data):
	# print(data, inspect.stack()[0][3])
	return data


def approvalPresaleArea(data):
	# print(data, inspect.stack()[0][3])
	return data


def averagePrice(data):
	# print(data, inspect.stack()[0][3])
	return data


def earliestStartDate(data):
	# print(data, inspect.stack()[0][3])
	return data


def completionDate(data):
	# print(data, inspect.stack()[0][3])
	return data


def earliestOpeningTime(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()

	df = pd.read_sql(con=Var.ENGINE,
					 sql="select min(EarliestOpeningDate) as col from PresellInfoItem where ProjectUUID='{"
						 "projectUUID}'".format(
						 projectUUID=data['ProjectUUID']))

	data['EarliestOpeningTime'] = str(df.col.values[0]) if not df.empty else ''

	return Row(**data)


def latestDeliversHouseDate(data):
	# print(data, inspect.stack()[0][3])
	return data


def presaleRegistrationManagementDepartment(data):
	# print(data, inspect.stack()[0][3])
	return data


def landLevel(data):
	# print(data, inspect.stack()[0][3])
	return data


def greeningRate(data):
	# print(data, inspect.stack()[0][3])
	return data


def floorAreaRatio(data):
	# print(data, inspect.stack()[0][3])
	return data


def managementFees(data):
	# print(data, inspect.stack()[0][3])
	return data


def managementCompany(data):
	# print(data, inspect.stack()[0][3])
	return data


def otheRights(data):
	# print(data, inspect.stack()[0][3])
	return data


def certificateOfUseOfStateOwnedLand(data):
	# print(data, inspect.stack()[0][3])
	return data


def constructionPermitNumber(data):
	# print(data, inspect.stack()[0][3])
	return data


def qualificationNumber(data):
	# print(data, inspect.stack()[0][3])
	return data


def landUsePermit(data):
	# print(data, inspect.stack()[0][3])
	return data


def buildingPermit(data):
	# print(data, inspect.stack()[0][3])
	return data


def legalPersonNumber(data):
	# print(data, inspect.stack()[0][3])
	return data


def legalPerson(data):
	# print(data, inspect.stack()[0][3])
	return data


def sourceUrl(data):
	# print(data, inspect.stack()[0][3])
	return data


def decoration(data):
	# print(data, inspect.stack()[0][3])
	return data


def parkingSpaceAmount(data):
	# print(data, inspect.stack()[0][3])
	return data


def remarks(data):
	# print(data, inspect.stack()[0][3])
	return data


def extraJson(data):
	# print(data, inspect.stack()[0][3])
	return data
