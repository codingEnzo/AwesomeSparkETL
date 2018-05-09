# coding=utf-8
from __future__ import unicode_literals
import json
import demjson
from . import Var
from pyspark.sql import Row


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


def dropColumn(data, columns=[]):
    if not isinstance(data, dict):
        data = data.asDict()
    for column in columns:
        del data[column]
    return Row(**data)
