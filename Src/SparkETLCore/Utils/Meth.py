# coding=utf-8
import datetime
import json
import demjson
import Var
import bisect
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
