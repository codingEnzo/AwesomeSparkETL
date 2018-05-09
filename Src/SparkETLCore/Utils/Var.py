# coding=utf-8
from __future__ import unicode_literals
from sqlalchemy import *
ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
NUMTAB = {f: t for f, t in zip('1234567890', '一二三四五六七八九零')}
