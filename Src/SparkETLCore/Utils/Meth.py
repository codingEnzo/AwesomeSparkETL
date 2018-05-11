# coding=utf-8
from __future__ import unicode_literals
import bisect
import datetime
import json
import demjson
from Var import *
from pyspark.sql import Row
from sqlalchemy import create_engine


def getEngine(dbName):
    eg = create_engine(
        'mysql+pymysql://root:gh001@10.30.1.70:3307/{db}?charset=utf8'.format(
            db=dbName))
    return eg


def jsonLoad(x):
    return demjson.decode(x)


def jsonDumps(x):
    return json.dumps(x, ensure_ascii=False)


def cleanName(x):
    x = x \
        .replace('（', '(').replace('）', ')') \
        .replace('】', ']').replace('【', '[') \
        .replace('，', ',').replace('－', '-') \
        .replace('〔', '[').replace('〕', ']')
    return x


def cleanBuildingStructure(x):
    x = x.replace('钢混', '钢混结构') \
        .replace('框架', '框架结构') \
        .replace('钢筋混凝土', '钢混结构') \
        .replace('混合', '混合结构') \
        .replace('砖混', '砖混结构') \
        .replace('框剪', '框架剪力墙结构') \
        .replace('结构结构', '结构')
    return x


def numberTable(x):
    return Var.NUMTAB.get(x, '')


def isInt(val):
    try:
        int(val)
    except ValueError:
        return False
    else:
        return True


def dateFormatter(dateString):
    ds = dateString.replace('/', '-')
    ds = datetime.datetime.strftime(dateString, "%Y-%m-%d")
    return ds


def bisectCheckFloorType(floor):
    floorstack = [-1, 0, 1, 3, 6, 11, 18, 32, 33]
    ix = bisect.bisect(floorstack, floor) - 1
    if ix < 0:
        ix = 0
    floortype = FLOORTYPES.get(floorstack[ix])
    return floortype


def getFloor(houseName):
    floor = '1'
    if isInt(houseName):
        _ = int(houseName)
        houseName = str(_)
        if (len(houseName) - 4) > 0:
            offset = len(houseName) - 4 if _ > 0 else len(houseName) - 3
            return '{}'.format(houseName[:offset])
        elif (len(houseName) - 2) > 0:
            offset = len(houseName) - 2 if _ > 0 else len(houseName) - 1
            return '{}'.format(houseName[:offset])
        else:
            return floor
    else:
        return '0'


def dropColumn(data, columns=[]):
    if not isinstance(data, dict):
        data = data.asDict()
    for column in columns:
        del data[column]
    return Row(**data)
