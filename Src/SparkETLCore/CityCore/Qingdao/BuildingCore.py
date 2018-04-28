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

METHODS = ['address',
		   'buildingArea',
		   'buildingAveragePrice',
		   'buildingCategory',
		   'buildingHeight',
		   'buildingId',
		   'buildingName',
		   'buildingPriceRange',
		   'buildingStructure',
		   'buildingType',
		   'buildingUUID',
		   'elevaltorInfo',
		   'elevatorHouse',
		   'estimatedCompletionDate',
		   'extrajson',
		   'floors',
		   'housingCount',
		   'isHasElevator',
		   'onTheGroundFloor',
		   'presalePermitNumber',
		   'projectName',
		   'realEstateProjectId',
		   'recordTime',
		   'remarks',
		   'sourceUrl',
		   'theGroundFloor',
		   'unitId',
		   'unitName',
		   'units',
		   'unsoldAmount']


def recordTime(data):
	# print(data, inspect.stack()[0][3])
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


def buildingName(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['BuildingName'] = Meth.cleanName(data['BuildingName'])
	return Row(**data)


def buildingId(data):
	# print(data, inspect.stack()[0][3])
	return data


def buildingUUID(data):
	# print(data, inspect.stack()[0][3])
	return data


def unitName(data):
	# print(data, inspect.stack()[0][3])
	return data


def unitId(data):
	# print(data, inspect.stack()[0][3])
	return data


def presalePermitNumber(data):
	# print(data, inspect.stack()[0][3])
	return data


def address(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select ProjectAddress as col from ProjectInfoItem where ProjectUUID='{projectUUID}' and ProjectAddress !='' "
		"order by RecordTime".format(
			projectUUID=data['ProjectUUID']), Var.ENGINE)
	data['Address'] = df.col.values[-1] if not df.empty else ''
	# data['Address'] = 'testAddress'.decode('utf-8') if not df.empty else ''
	return Row(**data)


def onTheGroundFloor(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and "
		"ActualFloor >= '1'".format(
			buildingUUID=data['BuildingUUID']), Var.ENGINE)

	data['OnTheGroundFloor'] = str(df.col.values[-1]) if df.col.values[-1] != 0 else ''
	return Row(**data)


def theGroundFloor(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}' and "
		"ActualFloor < '1'".format(
			buildingUUID=data['BuildingUUID']), Var.ENGINE)
	data['TheGroundFloor'] = str(df.col.values[-1]) if not df.empty else ''
	return Row(**data)


def estimatedCompletionDate(data):
	# print(data, inspect.stack()[0][3])
	return data


def housingCount(data):
	# print(data, inspect.stack()[0][3])
	# data = data.asDict()
	# df = pd.read_sql("select count(distinct HouseUUID) as col from HouseInfoItem where BuildingUUID='{
	# buildingUUID}'".format(
	#                          buildingUUID = data['BuildingUUID']),Var.ENGINE)
	# data['HousingCount'] = str(df.col.values[0]) if not df.empty else '0'
	# return Row(**data)
	return data


def floors(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select count(distinct ActualFloor) as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
			buildingUUID=data['BuildingUUID']), Var.ENGINE)
	data['Floors'] = str(df.col.values[0])
	return Row(**data)


def elevatorHouse(data):
	# print(data, inspect.stack()[0][3])
	return data


def isHasElevator(data):
	# print(data, inspect.stack()[0][3])
	return data


def elevaltorInfo(data):
	# print(data, inspect.stack()[0][3])
	return data


def buildingStructure(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select distinct BuildingStructure as col from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
			buildingUUID=data['BuildingUUID']), Var.ENGINE)
	data['BuildingStructure'] = Meth.jsonDumps(
		list(set(df.col.values) - set([''])))
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

	def getFloor(x):
		x_match = re.search(r'(\d+)', x)
		try:
			if x_match:
				res = int(x_match.group(1))
			else:
				res = 1
		except Exception as e:
			res = 1
		return res

	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql("select distinct ActualFloor from HouseInfoItem where BuildingUUID='{buildingUUID}'".format(
		buildingUUID=data['BuildingUUID']), Var.ENGINE)
	df['ActualFloor'] = df['ActualFloor'].apply(getFloor)
	data['BuildingType'] = check_floor_type(df.ActualFloor.agg('max')).decode('utf-8')
	return Row(**data)


def buildingHeight(data):
	return data


def buildingCategory(data):
	# print(data, inspect.stack()[0][3])
	return data


def units(data):
	# print(data, inspect.stack()[0][3])
	return data


def unsoldAmount(data):
	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	data['UnsoldAmount'] = str(data['UnsoldAmount'])
	return Row(**data)


def buildingAveragePrice(data):
	# print(data, inspect.stack()[0][3])
	return data


def buildingPriceRange(data):
	# print(data, inspect.stack()[0][3])
	return data


def buildingArea(data):
	def getMeasuredBuildingArea(x):
		x_match = re.search(r'(\d+[\.]\d+)', x)
		try:
			if x_match:
				res = float(x_match.group(1))
			else:
				res = 0.0
		except Exception as e:
			res = 0.0
		return res

	# print(data, inspect.stack()[0][3])
	data = data.asDict()
	df = pd.read_sql(
		"select MeasuredBuildingArea,HouseUUID from HouseInfoItem where BuildingUUID='{buildingUUID}' order by "
		"RecordTime desc".format(
			buildingUUID=data['BuildingUUID']), Var.ENGINE)
	if not df.empty:
		df['MeasuredBuildingArea'] = df.apply(
			lambda x: float(x['MeasuredBuildingArea']) if x['MeasuredBuildingArea'] else 0.0
			, axis=1)
		data['BuildingArea'] = str(df.MeasuredBuildingArea.sum())
	else:
		data['BuildingArea'] = ''
	return Row(**data)


def remarks(data):
	# print(data, inspect.stack()[0][3])
	return data


def sourceUrl(data):
	# print(data, inspect.stack()[0][3])
	return data


def extrajson(data):
	# print(data, inspect.stack()[0][3])
	return data
