# -*- coding: utf-8 -*-
import demjson

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from SparkETLCore.Utils.Meth import cleanName


@pandas_udf(StringType())
def record_time_clean(s):
    import datetime
    nt = datetime.datetime.now()
    s = s.apply(lambda t: nt.strftime("%Y-%m-%d %H:%M:%S") if not t else t)
    return s


@pandas_udf(StringType())
def city_clean(s):
    s = s.apply(lambda v: '徐州')
    return s


# ProjectInfoItem
@pandas_udf(StringType())
def district_name_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return r[0].replace('金山桥经济开发区', '经济技术开发区')

    s = s.apply(lambda x: func(x))
    return s


# ProjectInfoItem
@pandas_udf(StringType())
def address_apply(s):
    s = s.apply(lambda v: v[0])
    return s


@pandas_udf(StringType())
def building_structure_clean(s):
    s = s.apply(lambda x: x.replace('钢混', '钢混结构').replace('框架', '框架结构')
                .replace('钢筋混凝土', '钢混结构').replace('混合', '混合结构')
                .replace('结构结构', '结构').replace('砖混', '砖混结构')
                .replace('框剪', '框架剪力墙结构').replace('钢、', ''))
    return s


@pandas_udf(StringType())
def state_extract(s):
    def func(value):
        if not value:
            return '历史成交'
        else:
            return '明确成交'

    s = s.apply(lambda v: func(v))
    return s


# PresellInfoItem
@pandas_udf(StringType())
def region_name_extract(s):
    s = s.apply(lambda j: demjson.decode(j).get('ExtraRegionName', ''))
    return s


@pandas_udf(StringType())
def region_name_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return r[0] if r else ''

    s = s.apply(lambda x: func(x))
    return s


# PresalePermitNumber
@pandas_udf(StringType())
def presale_permit_number_clean(s):
    s = s.apply(lambda v: cleanName(v))
    return s
