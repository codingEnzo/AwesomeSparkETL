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
def building_id_extract(s):
    s = s.apply(lambda j: demjson.decode(j).get("ExtraBuildingID", ""))
    return s


# FROM HOUSE
@pandas_udf(StringType())
def unit_name_apply(s):
    def func(v):
        r = [i for i in v if i != '']
        r = list(set(','.join(r).split(',')))
        return demjson.encode(r)

    s = s.apply(lambda x: func(x))
    return s
