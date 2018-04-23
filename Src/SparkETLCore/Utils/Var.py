# coding=utf-8
from sqlalchemy import *
ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')
NUMTAB = {ord(f): ord(t) for f, t in zip('1234567890', '一二三四五六七八九')}
SYMBOLSTAB = {ord(f):ord(t) for f,t in zip('，。！？【】（）％＃＠＆１２３４５６７８９０',',.!?[]()%#@&1234567890')}