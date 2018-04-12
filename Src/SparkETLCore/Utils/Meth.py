# coding=utf-8
import json
import demjson
import Var


def jsonLoad(x):
    return demjson.decode(x)


def jsonDumps(x):
    return json.dumps(x, ensure_ascii=False)


def cleanName(x):
    x = x.encode('utf-8').replace('（', '(').replace('）', ')')\
        .replace('】', ']').replace('【', '[')\
        .replace('，', ',').replace('－', '-').\
        replace('〔', '[').replace('〕', ']').decode('utf-8')
    return x


def numberTable(x):
    return Var.NUMTAB.get(x, '')
