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

METHODS = ['address', 'balconys', 'buildingCompletedYear', 'buildingID', 'buildingName', 'buildingStructure',
		   'caseFrom', 'caseTime', 'districtName', 'dwelling', 'floor', 'floors', 'forecastBuildingArea',
		   'forecastInsideOfBuildingArea', 'forecastPublicArea', 'houseID', 'houseName', 'houseNumber', 'houseUseType',
		   'isAttachment', 'isMortgage', 'isMoveBack', 'isPrivateUse', 'isSharedPublicMatching',
           'measuredBuildingArea',
		   'measuredInsideOfBuildingArea', 'measuredSharedPublicArea', 'nominalFloor', 'presalePermitNumber', 'price',
		   'priceType', 'projectName','projectID',
		   'realEstateProjectID', 'regionName', 'remarks', 'sellSchedule', 'sellState', 'sourceLink', 'state',
		   'totalPrice', 'unenclosedBalconys', 'unitShape', 'unitStructure']


def address(data):
	data = data.asDict()
	data['Address'] = Meth.cleanName(data['Address'])
	return Row(**data)


def balconys(data):
	return data


def buildingCompletedYear(data):
	return data


def buildingID(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select BuildingID as col from BuildingInfoItem where BuildingUUID='{0}' "
						 "and BuildingID != '' limit 1".format(data['BuildingUUID']))
	data['BuildingID'] = df.col.values[-1] if not df.empty else ''
	return Row(**data)


def buildingName(data):
	data = data.asDict()
	data['BuildingName'] = Meth.cleanName(data['BuildingName'])
	return Row(**data)


def buildingStructure(data):
	return data


def caseFrom(data):
	return data


def caseTime(data):
	data = data.asDict()
	data['CaseTime'] = data['RecordTime']
	return Row(**data)


def districtName(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="SELECT DistrictName AS col FROM ProjectInfoItem WHERE ProjectUUID = '{0}' "
						 "AND DistrictName != '' LIMIT 1".format(data['ProjectUUID'])).fillna('')
	data['DistrictName'] = df.col.values[0] if not df.empty else ''
	return Row(**data)


def dwelling(data):
	data = data.asDict()
	data['Dwelling'] = Meth.cleanName(data['HouseUseType']) if data['HouseUseType'] != 'null' else ''
	return Row(**data)


def floor(data):
	data = data.asDict()
	if data['FloorName']:
		c = re.search('-?\d+', data['FloorName'])
		data['Floor'] = c.group() if c else ''
	return Row(**data)


def floors(data):
	return data


def forecastBuildingArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastBuildingArea'])
	data['ForecastBuildingArea'] = c.group() if c else ''
	return Row(**data)


def forecastInsideOfBuildingArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastInsideOfBuildingArea'])
	data['ForecastInsideOfBuildingArea'] = c.group() if c else ''
	return Row(**data)


def forecastPublicArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['ForecastPublicArea'])
	data['ForecastPublicArea'] = c.group() if c else ''
	return Row(**data)



def houseID(data):
	return data


def houseName(data):
	data = data.asDict()
	data['HouseName'] = Meth.cleanName(data['HouseName'])
	return Row(**data)


def houseNumber(data):
	return data


def houseUseType(data):
	data = data.asDict()
	data['HouseUseType'] = Meth.cleanName(data['HouseUseType'])
	return Row(**data)


def isAttachment(data):
	return data


def isMortgage(data):
	return data


def isMoveBack(data):
	return data


def isPrivateUse(data):
	return data


def isSharedPublicMatching(data):
	return data
def measuredBuildingArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredBuildingArea'])
	data['MeasuredBuildingArea'] = c.group() if c else ''
	return Row(**data)


def measuredInsideOfBuildingArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredInsideOfBuildingArea'])
	data['MeasuredInsideOfBuildingArea'] = c.group() if c else ''
	return Row(**data)


def measuredSharedPublicArea(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['MeasuredSharedPublicArea'])
	data['MeasuredSharedPublicArea'] = c.group() if c else ''
	return Row(**data)


def nominalFloor(data):
	data = data.asDict()
	data['NominalFloor'] = Meth.cleanName(data['FloorName'])
	return Row(**data)


def presalePermitNumber(data):
	return data


def price(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['SalePriceByBuildingArea'])
	data['Price'] = c.group() if c else ''
	return Row(**data)


def priceType(data):
	data = data.asDict()
	data['PriceType'] = '预售方案备案单价'.decode('utf-8')
	return Row(**data)


def projectName(data):
	data = data.asDict()
	if data['ProjectName']:
		data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	else:
		df = pd.read_sql(con=Var.ENGINE,
						 sql="SELECT ProjectName AS col FROM ProjectInfoItem WHERE ProjectUUID = '{0}' "
							 "AND ProjectName !='' LIMIT 1".format(data['ProjectUUID']))
		data['ProjectName'] = df.col.values[0] if not df.empty else ''
	return Row(**data)

def projectID(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql=" SELECT ProjectID as col FROM ProjectInfoItem WHERE ProjectUUID = '{projectUUID}'  AND "
						 "ProjectID !='' LIMIT 1 ".format(projectUUID=data['ProjectUUID']))
	data['ProjectID'] = df.col.values[0] if not df.empty else ''
	return Row(**data)

def realEstateProjectID(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql=" select RealEstateProjectID as col from ProjectInfoItem where ProjectUUID = '{projectUUID}' "
						 "and RealEstateProjectID !='' limit 0,1 ".format(projectUUID=data['ProjectUUID']))
	data['RealEstateProjectID'] = str(df.col.values[0]) if not df.empty else ''
	return Row(**data)


def regionName(data):
	return data


def remarks(data):
	return data


def sellSchedule(data):
	return data


def sellState(data):
	return data


def sourceLink(data):
	return data


def state(data):
	data = data.asDict()
	data['State'] = '明确退房'.decode('utf-8')
	return Row(**data)


def totalPrice(data):
	data = data.asDict()
	c = re.search('([1-9]\d*\.\d*|0\.\d*[1-9]\d*)|\d+', data['TotalPrice'])
	data['TotalPrice'] = c.group() if c else ''
	return Row(**data)


def unenclosedBalconys(data):
	return data


def unitShape(data):
	data = data.asDict()
	data['UnitShape'] = data['UnitShape'].translate(Var.NUMTAB)
	return Row(**data)


def unitStructure(data):
	return data
