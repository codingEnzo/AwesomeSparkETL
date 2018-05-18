# -*- coding: utf-8 -*-
import demjson
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf


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

    s = s.apply(lambda x: fun(x))
    return s


# ProjectInfoItem
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


@pandas_udf(StringType())
def presale_permit_number_clean(s):
    s = s.apply(lambda v: v.replace('（', '(').replace('）', ')'))
    return s


def price(data):
    data = data.asDict()
    pid = data['ProjectUUID']
    sql = """
    SELECT ProjectInfoItem.AveragePrice FROM ProjectInfoItem
    WHERE ProjectInfoItem.ProjectUUID != {}
    """.format(pid)
    return data


@pandas_udf(StringType())
def state_extract(s):
    def func(value):
        if values == '可销售':
            return '明确成交'
        else:
            return '历史成交'

    s = s.apply(lambda v: func(v))
    return s


# price = ProjeDF['AveragePrice'][(ProjeDF.ProjectUUID==temple['ProjectUUID'])&(ProjeDF.RecordTime <= deal_case.casetime)]
# districtname = ProjeDF['DistrictName'][(ProjeDF.ProjectUUID==temple['ProjectUUID'])&(ProjeDF.DistrictName !='')]
# address = ProjeDF['ProjectAddress'][(ProjeDF.ProjectUUID==temple['ProjectUUID'])&(ProjeDF.ProjectAddress!='')]
# regionname = PresellInfoDF['ExtraRegionName'][(PresellInfoDF.PresalePermitNumber==temple['PresalePermitNumber'])&(PresellInfoDF.ExtraRegionName !='')]


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
        return r[0]

    s = s.apply(lambda x: func(x))
    return s


# PresalePermitNumber
@pandas_udf(StringType())
def presale_permit_number_clean(s):
    s = s.apply(lambda v: cleanName(v))
    return s
