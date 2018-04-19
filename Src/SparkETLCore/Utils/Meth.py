# coding=utf-8
import bisect
import datetime
import json
import re

import demjson
from sqlalchemy import create_engine

import Var


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
    x = x.encode('utf-8') \
        .replace('（', '(').replace('）', ')') \
        .replace('】', ']').replace('【', '[') \
        .replace('，', ',').replace('－', '-') \
        .replace('〔', '[').replace('〕', ']').decode('utf-8')
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
    floorstack = [3, 6, 11, 18, 32, 33]
    ix = bisect.bisect(floorstack, floor) - 1
    floortype = Var.FLOORTYPES.get(floorstack[ix])
    return floortype


def getFloor(houseName):
    hName = houseName.decode('utf8').lower().replace('铺', '').replace('阁', ''). \
        replace('库', '').replace('跃', ''). \
        replace('j', '').replace('车', ''). \
        replace('商', '').replace('楼', ''). \
        replace('gl', '').replace('g1', '').encode('utf-8')
    floor = '1'
    if isInt(hName):
        _ = int(hName)
        if (len(hName) - 4) > 0:
            offset = len(hName) - 4 if _ > 0 else len(hName) - 3
            return '{}'.format(hName[:offset])
        elif (len(hName) - 2) > 0:
            offset = len(hName) - 2 if _ > 0 else len(hName) - 1
            return '{}'.format(hName[:offset])
    else:
        if re.search(r'^[a-z]', hName):
            if re.search(r'\d+', hName):
                _floor = re.search(r'\d+', hName).group(0)
                if (len(_floor) - 4) > 0:
                    offset = len(_floor) - 4
                    return '{}'.format(_floor[:offset])
                elif (len(_floor) - 2) > 0:
                    offset = len(_floor) - 2
                    return '{}'.format(_floor[:offset])
        elif '-' in hName:
            if hName.startswith('-35-'):
                _ = re.search(r'\-.+\-(.+)', hName)
                if _:
                    _floor = _.group(1)
                    if len(_floor) - 2 > 0:
                        return _floor[1]
                    else:
                        return '-1'
            elif hName.startswith('-'):
                if re.search(
                        r'^-[a-z]',
                        hName) or len(hName) < 4 or hName.startswith('-0'):
                    return '-1'
                elif re.search(r'^-[1-9]', hName):
                    return hName[:1]
            else:
                _floor = re.search(r'-(.+)', hName).group(1)
                _floor = re.sub(r'[\u4E00-\u9FA5]', '', _floor)
                if _floor.count('-') == 2:
                    _floor = re.search(r'-(\d+)-', _floor).group(1)
                elif (not _floor.startswith('0')) and (not re.search(
                        r'^[a-z]', _floor)):
                    if len(_floor) - 4 > 0:
                        offset = len(_floor) - 4
                        return _floor[:offset]
                    elif len(_floor) - 2 > 0:
                        offset = len(_floor) - 4
                        return _floor[:offset]

        elif re.search(r'[\u4E00-\u9FA5]', hName):
            if '号' in hName or '单元' in hName:
                _ = re.search(r'号(\d+)|单元(\d+)', hName)
                if _:
                    _floor = _.group(1)
                    if (len(_floor) - 5 < 0) and (len(_floor) - 2 > 0):
                        gap = len(_floor) - 2
                        floor = _floor[:gap]
    return floor
