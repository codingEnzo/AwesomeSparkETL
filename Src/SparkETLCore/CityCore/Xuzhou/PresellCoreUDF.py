# -*- coding: utf-8 -*-
import demjson
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType

from SparkETLCore.Utils.Meth import cleanName


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


def lssueDate(data):
    ds = data['LssueDate']
    _row = data.asDict()
    _row['LssueDate'] = dateFormatter(ds)
    data = Row(**_row)
    return data


@pandas_udf(StringType())
def land_use_clean(s):
    s = s.apply(
        lambda v: demjson.encode(v.replace('、', '/').replace('，', ',').split(','))
    )
    return s


# 直接 .agg('count')
# def presaleHouseCount(data):
#     data = data.asDict()
#     permit_num = data.get('PresalePermitNum', "")
#     sql = """
#     SELECT HouseInfoItem.MeasuredBuildingArea FROM HouseInfoItem
#     JOIN PresellInfoItem ON HouseInfoItem.ProjectUUID = PresellInfoItem.ProjectUUID
#     WHERE PresellInfoItem.PresalePermitNumber = "{}"
#     """.format(permit_num)
#     q = pd.read_sql(sql, ENGINE)
#     h_count = 0
#     if not q.empty:
#         h_count = q['MeasuredBuildingArea'].count()
#     data['presaleHouseCount'] = h_count
#     data = Row(**data)
#     return data
