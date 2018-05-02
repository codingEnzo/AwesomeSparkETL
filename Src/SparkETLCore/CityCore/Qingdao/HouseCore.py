# coding=utf-8
from __future__ import division
import sys
import datetime
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')
sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Meth, Var, Config

METHODS = ['actualFloor',
		   'address',
		   'balconys',
		   'buildingID',
		   'buildingName',
		   'buildingStructure',
		   'caseTime',
		   'city',
		   'decoration',
		   'decorationPrice',
		   'districtName',
		   'dwelling',
		   'extraJson',
		   'floorCount',
		   'floorHight',
		   'floorName',
		   'floorType',
		   'forecastBuildingArea',
		   'forecastInsideOfBuildingArea',
		   'forecastPublicArea',
		   'halls',
		   'houseId',
		   'houseLabel',
		   'houseLabelLatest',
		   'houseName',
		   'houseNature',
		   'houseNumber',
		   'houseSalePrice',
		   'houseShape',
		   'houseState',
		   'houseStateLatest',
		   'houseType',
		   'houseUUID',
		   'houseUseType',
		   'isAttachment',
		   'isMortgage',
		   'isMoveBack',
		   'isPrivateUse',
		   'isSharedPublicMatching',
		   'kitchens',
		   'measuredBuildingArea',
		   'measuredInsideOfBuildingArea',
		   'measuredSharedPublicArea',
		   'measuredUndergroundArea',
		   'natureOfPropertyRight',
		   'price',
		   'priceType',
		   'projectName',
		   'realEstateProjectId',
		   'recordTime',
		   'remarks',
		   'rooms',
		   'salePriceByBuildingArea',
		   'salePriceByInsideOfBuildingArea',
		   'sellSchedule',
		   'sellState',
		   'sourceUrl',
		   'toilets',
		   'totalPrice',
		   'toward',
		   'unenclosedBalconys',
		   'unitId',
		   'unitName',
		   'unitShape',
		   'unitStructure']


def recordTime(data):
	return data


def caseTime(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	# data['CaseTime'] = Meth.jsonLoad(data['ExtraJson']).get('ExtraCurTimeStamp', str(datetime.datetime.now()))
	data['CaseTime'] = data['RecordTime']
	return Row(**data)


def projectName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	return Row(**data)


def realEstateProjectId(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql=" select RealEstateProjectID as col from ProjectInfoItem where ProjectUUID = '{projectUUID}' "
                         "and RealEstateProjectID !='' limit 0,1 ".format(
						 projectUUID=data['ProjectUUID']))
	data['RealEstateProjectID'] = str(df.col.values[0]) if not df.empty else ''
	return Row(**data)


def buildingName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['BuildingName'] = Meth.cleanName(data['BuildingName'])
	return Row(**data)


def buildingID(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select BuildingID as col from BuildingInfoItem where BuildingUUID='{buildingUUID}' limit 1"\
					 .format(buildingUUID=data['BuildingUUID']))
	data['BuildingID'] = df.col.values[-1] if not df.empty else ''
	return Row(**data)


def city(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['City'] = '青岛'
	return Row(**data)


def districtName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select DistrictName as col from ProjectInfoItem where ProjectUUID='{projectUUID}' order by "
                         "RecordTime DESC limit 1".format(
						 projectUUID=data['ProjectUUID']))
	data['DistrictName'] = Meth.cleanName(df.col.values[-1]) if not df.empty else ''
	return Row(**data)


def unitName(data):
	return data


def unitId(data):
	return data


def houseNumber(data):
	# print(data, inspect.stack()[0][3])
	return data


def houseName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['HouseName'] = data['HouseName']
	return Row(**data)


def houseId(data):
	return data


def houseUUID(data):
	return data


def address(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select ProjectAddress as col from ProjectInfoItem where ProjectUUID='{projectUUID}' order by "
                         "RecordTime limit 1".format(
						 projectUUID=data['ProjectUUID']))
	data['Address'] = df.col.values[-1] if not df.empty else ''
	# data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
	return Row(**data)


def floorName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['FloorName'] = data['FloorName']
	return Row(**data)


def actualFloor(data):
	# print(data, inspect.stack()[0][3])
	return data


def floorCount(data):
	# print(data, inspect.stack()[0][3])
	return data


def floorType(data):
	return data


def floorHight(data):
	# print(data, inspect.stack()[0][3])
	return data


def unitShape(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['UnitShape'] = data['UnitShape'].translate(Var.NUMTAB)
	return Row(**data)


def unitStructure(data):
	return data


def rooms(data):
	return data


def halls(data):
	return data


def kitchens(data):
	# print(data, inspect.stack()[0][3])
	return data


def toilets(data):
	# print(data, inspect.stack()[0][3])
	return data


def balconys(data):
	# print(data, inspect.stack()[0][3])
	return data


def unenclosedBalconys(data):
	# print(data, inspect.stack()[0][3])
	return data


def houseShape(data):
	data = data.asDict()
	data['HouseShape'] = data['HouseShape'].replace('null', '')
	return Row(**data)


def dwelling(data):
	return data


def forecastBuildingArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['ForecastBuildingArea'] = data['ForecastBuildingArea']
	return Row(**data)


def forecastInsideOfBuildingArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['ForecastInsideOfBuildingArea'] = data['ForecastInsideOfBuildingArea']
	return Row(**data)


def forecastPublicArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['ForecastPublicArea'] = data['ForecastPublicArea']
	return Row(**data)


def measuredBuildingArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['MeasuredBuildingArea'] = data['MeasuredBuildingArea']
	return Row(**data)


def measuredInsideOfBuildingArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['MeasuredInsideOfBuildingArea'] = data['MeasuredInsideOfBuildingArea']
	return Row(**data)


def measuredSharedPublicArea(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['MeasuredSharedPublicArea'] = data['MeasuredSharedPublicArea']
	return Row(**data)


def measuredUndergroundArea(data):
	return data


def toward(data):
	return data


def houseType(data):
	return data


def houseNature(data):
	return data


def decoration(data):
	return data


def natureOfPropertyRight(data):
	return data


def houseUseType(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['HouseUseType'] = Meth.cleanName(data['HouseUseType']) if data['HouseUseType'] != 'null' else ''
	return Row(**data)


def buildingStructure(data):
	# print(data, inspect.stack()[0][3])
	return data


def houseSalePrice(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select BuildingAveragePrice as col from BuildingInfoItem where BuildingUUID = '{"
                         "buildingUUID} order by RecordTime desc limit 1 '")
	data['HouseSalePrice'] = df.col.values[-1] if not df.empty else ''
	return Row(**data)


def salePriceByBuildingArea(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select BuildingAveragePrice as col from BuildingInfoItem where BuildingUUID = '{"
                         "buildingUUID} order by RecordTime desc limit 1 '")
	data['SalePriceByBuildingArea'] = df.col.values[-1] if not df.empty else ''
	return Row(**data)


def salePriceByInsideOfBuildingArea(data):
	return data


def isMortgage(data):
	# print(data, inspect.stack()[0][3])
	return data


def isAttachment(data):
	# print(data, inspect.stack()[0][3])
	return data


def isPrivateUse(data):
	# print(data, inspect.stack()[0][3])
	return data


def isMoveBack(data):
	# print(data, inspect.stack()[0][3])
	return data


def isSharedPublicMatching(data):
	# print(data, inspect.stack()[0][3])
	return data


def sellState(data):
	return data


def sellSchedule(data):
	return data


def houseState(data):
	return data


def houseStateLatest(data):
	return data


def houseLabel(data):
	return data


def houseLabelLatest(data):
	return data


def totalPrice(data):
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select BuildingAveragePrice as col from BuildingInfoItem where BuildingUUID = '{"
                         "buildingUUID} order by RecordTime desc limit 1 '")
	if not df.empty:
		data = data.asDict()
		if data['MeasuredBuildingArea'] != '':
			price = float(df.col.values[-1])
			area = float(data['MeasuredBuildingArea'])
			data['TotalPrice'] = str(price * area)
			return Row(**data)
	return data


def price(data):
	return data


def priceType(data):
	data = data.asDict()
	data['PriceType'] = '预售单价'.decode('utf-8')
	return Row(**data)


def decorationPrice(data):
	return data


def remarks(data):
	return data


def sourceUrl(data):
	return data


def extraJson(data):
	return data
