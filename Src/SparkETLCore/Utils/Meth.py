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
    x = x.replace('（', '(').replace('）', ')')\
        .replace('】', ']').replace('【', '[')\
        .replace('，', ',').replace('－', '-').\
        replace('〔', '[').replace('〕', ']')
    return x

def isInt(val):
    try:
        int(val)
    except ValueError:
        return False
    else:
        return True

def cleanUnit(x):
    x = x.replace('㎡','').replace('万','0000')\
            .replace('待定','').replace('元/','').replace('/','-')\
            .replace('%','').replace(' ','').replace('\t','')\
            .replace('\n','').replace('无','')
    return x

def cleanDate(x):
    rule = re.compile(r'\d{4}\-\d{1,2}\-\d{1,2}|\d{4}\-\d{1,2}')
    x = x.replace('年','-').replace('月','-')\
                        .replace('.','-').replace('日','')
    if rule.search(x):
        x = rule.search(x).group()
    else:
        x =''
    return x