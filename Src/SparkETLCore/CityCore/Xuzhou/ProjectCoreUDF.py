# -*- coding: utf-8 -*-
import demjson

from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType

from SparkETLCore.Utils.Meth import cleanName


@pandas_udf(StringType())
def record_time_clean(s):
    import datetime
    nt = datetime.datetime.now()
    s = s.apply(lambda t: nt.strftime("%Y-%m-%d %H:%M:%S") if not t else t)
    return s


@pandas_udf(StringType())
def region_name_extract(s):
    s = s.apply(lambda j: demjson.decode(j).get('ExtraRegionName', ''))
    return s


@pandas_udf(StringType())
def region_name_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def land_use_clean(s):
    s = s.apply(
        lambda x: x.replace('宅', '住宅') \
        .replace('宅宅', '宅') \
        .replace('住住', '住') \
        .replace('、', '/') \
        .replace('，', ',').strip('/,')
    )
    return s


@pandas_udf(StringType())
def land_use_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def house_use_type_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def lssue_date_clean(s):
    s = s.apply(lambda x: cleanName(x))
    return s


@pandas_udf(StringType())
def lssue_date_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def presale_permit_number_clean(s):
    s = s.apply(lambda x: cleanName(x))
    return s


@pandas_udf(StringType())
def presale_permit_number_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def cert_state_land_extract(s):
    s = s.apply(
        lambda j: demjson.decode(j).get('ExtraCertificateOfUseOfStateOwnedLand', '')
    )
    return s


@pandas_udf(StringType())
def cert_state_land_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def construction_permit_number_extract(s):
    s = s.apply(
        lambda j: demjson.decode(j).get('ExtraConstructionPermitNumber', '')
    )
    return s


@pandas_udf(StringType())
def construction_permit_number_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s


@pandas_udf(StringType())
def land_use_permit_extract(s):
    s = s.apply(
        lambda j: demjson.decode(j).get("ExtraLandCertificate", "").replace("、", "")
    )
    return s


@pandas_udf(StringType()):
def land_use_permit_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s
