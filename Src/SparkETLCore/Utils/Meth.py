# coding=utf-8
import json
import demjson
from . import Var


def jsonLoad(x):
    return demjson.decode(x)


def jsonDumps(x):
    return json.dumps(x, ensure_ascii=False)


def cleanName(x):
    x = x.replace('（', '(').replace('）', ')')\
        .replace('】', ']').replace('【', '[')\
        .replace('，', ',').replace('－', '-').\
        replace('〔', '[').replace('〕', ']')
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
