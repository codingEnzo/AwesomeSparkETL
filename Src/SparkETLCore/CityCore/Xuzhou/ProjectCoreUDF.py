# -*- coding: utf-8 -*-
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType


@pandas_udf(StringType())
def land_use_clean(s):
    if s:
        s = s.replace('宅', '住宅') \
            .replace('宅宅', '宅') \
            .replace('住住', '住') \
            .replace('、', '/') \
            .replace('，', ',').strip('/,')
    return s


@pandas_udf('ProjectUUID string, LandUse string', PandasUDFType.GROUPED_MAP)
def land_use_group(pdf):
    import demjson
    land_use = pdf.LandUse.distinct()
    return pdf.assign(LandUse=demjson.encode(land_use))


@pandas_udf('ProjectUUID string, ExtraJson string', PandasUDFType.GROUPED_MAP)
def region_name_group(pdf):
    extra_json = pdf.ExtraJson


def region_name_parse(s):
    if s:
        pass
