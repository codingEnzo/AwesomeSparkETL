# -*- coding: utf-8 -*-
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType


@pandas_udf(StringType())
def city_clean(s):
    s = s.apply(lambda v: '徐州')
    return s


@pandas_udf(StringType())
def district_name_apply(s):
    s = s.apply(lambda v: v[0].replace('金山桥经济开发区', '经济技术开发区'))
    return s


@pandas_udf(StringType())
def address_apply(s):
    s = s.apply(lambda v: v[0])
    return s


@pandas_udf(StringType())
def building_structure_clean(s):
    s = s.apply(
        lambda x: x.replace('钢混', '钢混结构') \
        .replace('框架', '框架结构') \
        .replace('钢筋混凝土', '钢混结构') \
        .replace('混合', '混合结构') \
        .replace('结构结构', '结构') \
        .replace('砖混', '砖混结构') \
        .replace('框剪', '框架剪力墙结构') \
        .replace('钢、', '')
    )
    return s
