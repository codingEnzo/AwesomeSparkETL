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
	if data['ProjectName']:
		data['ProjectName'] = Meth.cleanName(data['ProjectName'])
	else:
		df = pd.read_sql(con=Var.ENGINE,
						 sql="SELECT ProjectName AS col FROM ProjectInfoItem WHERE "
							 "ProjectUUID = '{0}' AND ProjectName != '' LIMIT 1".format(data['ProjectUUID']))
		data['ProjectName'] = Meth.cleanName(df.col.values[0]) if not df.empty else ''
	return Row(**data)


def realEstateProjectID(data):
	return data


def buildingName(data):
	data = data.asDict()
	data['BuildingName'] = Meth.cleanName(data['BuildingName'])
	return Row(**data)


def buildingID(data):
	return data


def buildingUUID(data):
	return data


def unitName(data):
	data = data.asDict()
	data['UnitName'] = Meth.cleanName(data['UnitName'])
	return Row(**data)


def unitID(data):
	return data


def presalePermitNumber(data):
	data = data.asDict()
	arr = data['PresalePermitNumber'].replace('，'.decode('utf-8'),',').split(',')
	data['PresalePermitNumber'] = Meth.jsonDumps(filter(lambda x: x and x.strip(), arr))
	return Row(**data)


def address(data):
	data = data.asDict()
	if data['Address']:
		data['Address'] = Meth.cleanName(data['Address'])
	else:
		df = pd.read_sql(con=Var.ENGINE,
						 sql="SELECT ProjectAddress AS col FROM ProjectInfoItem WHERE ProjectUUID = '{0}' LIMIT 1" \
						 .format(data['ProjectUUID']))
		data['Address'] = Meth.cleanName(df.col.values[0]) if not df.empty else ''
	return Row(**data)


def onTheGroundFloor(data):
	def getNumber(x):
		c = re.search('-?\d+', x)
		return int(c.group()) if c else 0
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select DISTINCT(FloorName) as col from HouseInfoItem where BuildingUUID='{0}'" \
					 .format(data['BuildingUUID']))
	if not df.empty:
		df['col'] = df['col'].apply(getNumber)
		data['OnTheGroundFloor'] = str(df['col'][df['col']>=0].size)
	else:
		data['OnTheGroundFloor']=''
	return Row(**data)


def theGroundFloor(data):
	return data


def estimatedCompletionDate(data):
	return data


def housingCount(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select count(HouseUUID) as col from HouseInfoItem where "
						 "BuildingUUID='{0}'".format(data['BuildingUUID']))
	data['HousingCount'] = str(df.col.values[0])
	return Row(**data)


def floors(data):
	data = data.asDict()
	df = pd.read_sql(con=Var.ENGINE,
					 sql="select DISTINCT(FloorName) as col from HouseInfoItem where BuildingUUID='{0}'" \
					 .format(data['BuildingUUID']))
	data['Floors'] = str(df.col.values.size) if not df.empty else ''
	return Row(**data)


def buildingStructure(data):
	return data


def buildingType(data):
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
	data = data.asDict()
	if not data['SourceUrl']:
		data['SourceUrl'] = Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingURL', '')
	return Row(**data)


def extrajson(data):
	return data
