# coding=utf-8
from __future__ import unicode_literals
import re
import bisect
import datetime
import json
import demjson
from SparkETLCore.Utils import Var
from pyspark.sql import Row


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
    return NUMTAB_TRANS.get(x, '')


def isInt(val):
    try:
        int(val)
    except ValueError:
        return False
    else:
        return True


def dateFormatter(dateString):
    ds = dateString.replace('/', '-')
    ds = datetime.datetime.strptime(dateString, "%Y-%m-%d")
    return ds


def bisectCheckFloorType(floor):
    floorstack = [-1, 0, 1, 3, 6, 11, 18, 32, 33]
    ix = bisect.bisect(floorstack, floor) - 1
    if ix < 0:
        ix = 0
    floortype = Var.FLOORTYPES.get(floorstack[ix])
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


def cleanUnit(x):
    x = x.replace('㎡', '').replace('万', '0000')\
        .replace('待定', '').replace('元/', '').replace('/', '-')\
        .replace('%', '').replace(' ', '').replace('\t', '')\
        .replace('\n', '').replace('无', '')
    return x


def cleanDate(x):
    rule = re.compile(r'\d{4}\-\d{1,2}\-\d{1,2}|\d{4}\-\d{1,2}')
    x = x.replace('年', '-').replace('月', '-')\
        .replace('.', '-').replace('日', '')
    if rule.search(x):
        x = rule.search(x).group()
    else:
        x = ''
    return x
