# -*- coding: utf-8 -*-
import demjson
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType

from SparkETLCore.Utils.Meth import cleanName, dateFormatter


@pandas_udf(StringType())
def record_time_clean(s):
    import datetime
    nt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s = s.apply(lambda t: nt if not t else t)
    return s


@pandas_udf(StringType())
def presale_permit_number_clean(s):
    s = s.apply(lambda v: cleanName(v))
    return s


@pandas_udf(StringType())
def presale_building_amount_clean(s):
    def reshape(value):
        v_list = value.replace('、', ',').split(',')
        v_len = len(set(v_list))
        return str(v_len)

    s = s.apply(lambda v: reshape(v))
    return s


@pandas_udf(StringType())
def lssue_date_clean(s):
    s = s.apply(lambda v: dateFormatter(str(v)))
    return s


@pandas_udf(StringType())
def land_use_clean(s):
    s = s.apply(
        lambda v: demjson.encode(v.replace('、', '/').replace('，', ',').split(','))
    )
    return s
