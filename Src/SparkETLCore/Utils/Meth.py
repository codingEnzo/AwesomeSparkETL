# coding=utf-8
import os
import sys
import json
import demjson
import re
# import Var
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

# def numberTable(x):
#     return Var.NUMTAB.get(x, '')


def isInt(val):
    try:
        int(val)
    except ValueError:
        return False
    else:
        return True

def cleanUnit(x):
    x = x.encode('utf-8').replace('㎡','').replace('万','0000')\
            .replace('待定','').replace('元/','').replace('/','-')\
            .replace('%','').replace(' ','').replace('\t','')\
            .replace('\n','').replace('无','').decode('utf-8')
    return x

def cleanDate(x):
    rule = re.compile(r'\d{4}\-\d{1,2}\-\d{1,2}|\d{4}\-\d{1,2}')
    x = x.encode('utf-8').replace('年','-').replace('月','-')\
                        .replace('.','-').replace('日','').decode('utf-8')
    if rule.search(x):
        x = rule.search(x).group()
    else:
        x =''
    return x