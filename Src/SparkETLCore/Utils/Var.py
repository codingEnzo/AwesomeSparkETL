# coding=utf-8
from sqlalchemy import create_engine

ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
NUMTAB = {ord(f): ord(t) for f, t in zip('1234567890', '一二三四五六七八九')}
FLOORTYPES = {
    3: '低层(1-3)',
    6: '多层(4-6)',
    11: '小高层(7-11)',
    18: '中高层(12-18)',
    32: '高层(19-32)',
    33: '超高层(33)',
}
