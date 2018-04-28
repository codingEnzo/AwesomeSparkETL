# coding=utf-8
from __future__ import division

import re
import sys
import inspect
import pandas as pd
import numpy as np

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')

from pyspark.sql import Row
from Utils import Var, Meth, Config

METHODS = ['address',
		   'buildingArea',
		   'buildingAveragePrice',
		   'buildingCategory',
		   'buildingHeight',
		   'buildingID',
		   'buildingName',
		   'buildingPriceRange',
		   'buildingStructure',
		   'buildingType',
		   'buildingUUID',
		   'estimatedCompletionDate',
		   'extrajson',
		   'floors',
		   'housingCount',
		   'onTheGroundFloor',
		   'presalePermitNumber',
		   'projectName',
		   'realEstateProjectID',
		   'recordtime',
		   'remarks',
		   'sourceUrl',
		   'theGroundFloor',
		   'unitID',
		   'unitName',
		   'units',
		   'unsoldAmount']


def recordtime(data):
	return data


def projectName(data):
	data = data.asDict()
	data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	return Row(**data)


def realEstateProjectID(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select ExtraJson as col from ProjectInfoItem where ProjectUUID='{projectUUID}' limit 1".format(
						 projectUUID=data['ProjectUUID']))
	data['RealEstateProjectID'] = Meth.jsonLoad(df.col.values[0]).get('ExtraPropertyID', '')
	return Row(**data)


def buildingName(data):
	data = data.asDict()
	data['BuildingName'] = Meth.cleanName(str(data['BuildingName']))
	return Row(**data)


def buildingID(data):
	data = data.asDict()
	data['BuildingID'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingID', ''))
	return Row(**data)


def buildingUUID(data):
	return data


def unitName(data):
	data = data.asDict()
	data['unitName'] = Meth.cleanName(data['unitName'])
	return Row(**data)


def unitID(data):
	return data


def presalePermitNumber(data):
	data = data.asDict()
	data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
	return Row(**data)


def address(data):
	data = data.asDict()
	data['Address'] = Meth.cleanName(data['Address'])
	return Row(**data)


def onTheGroundFloor(data):
	return data


def theGroundFloor(data):
	return data


def estimatedCompletionDate(data):
	return data


def housingCount(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select count(HouseUUID) as col from HouseInfoItem where BuildingUUID='{"
						 "buildingUUID}'".format(
						 buildingUUID=data['BuildingUUID']))
	data['HousingCount'] = str(df.col.values[0])
	return Row(**data)


def floors(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select ExtraBuildingFloor as col from HouseInfoItem where BuildingUUID='{"
						 "buildingUUID}'".format(
						 buildingUUID=data['BuildingUUID']))
	string = str(Meth.jsonLoad(df.col.values[0]['ExtraJson']).get('ExtraBuildingFloor', ''))
	data['Floors'] = re.search(r'\d+', string).group() if re.search(r'\d+', string) else ''
	return Row(**data)


def buildingStructure(data):
	return data


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
	if data['Floors']:
		data['BuildingType'] = check_floor_type(data['Floors']).decode('utf-8')
		return Row(**data)
	else:
		return data


def buildingHeight(data):
	return data


def buildingCategory(data):
	return data


def units(data):
	return data


def unsoldAmount(data):
	return data


def buildingAveragePrice(data):
	return data


def buildingPriceRange(data):
	return data


def buildingArea(data):
	return data


def remarks(data):
	return data


def sourceUrl(data):
	return data


def extrajson(data):
	return data
