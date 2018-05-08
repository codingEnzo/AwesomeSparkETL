# coding=utf-8
import os 
import pandas
from sqlalchemy import *
# sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')
NUMTAB = {ord(f): ord(t) for f, t in zip('1234567890', '一二三四五六七八九')}
SYMBOLSTAB = {ord(f):ord(t) for f,t in zip('，。！？【】（）％＃＠＆１２３４５６７８９０',',.!?[]()%#@&1234567890')}


if __name__ == '__main__':
  pt = pandas.read_sql('SELECT * FROM ProjectInfoItem LIMIT 1',con=ENGINE)
  pm = pandas.read_sql('SELECT * FROM project_info_hefei LIMIT 1',con=MIRROR_ENGINE)
  
  prsellt = pandas.read_sql('SELECT * FROM PresellInfoItem LIMIT 1',con=ENGINE)
  prsellm = pandas.read_sql('SELECT * FROM permit_info_hefei LIMIT 1',con=MIRROR_ENGINE)
  
  bt = pandas.read_sql('SELECT * FROM BuildingInfoItem LIMIT 1',con=ENGINE)
  bm = pandas.read_sql('SELECT * FROM building_info_hefei LIMIT 1',con=MIRROR_ENGINE)

  ht = pandas.read_sql('SELECT * FROM HouseInfoItem LIMIT 1',con=ENGINE)
  # hm = pandas.read_sql('SELECT * FROM house_info_hefei LIMIT 1',con=MIRROR_ENGINE)
  dc = pandas.read_sql('SELECT * FROM deal_case_hefei LIMIT 1',con=MIRROR_ENGINE)
  # print (set(pt.columns))
  # print (set(pm.columns))
  # print (set(pt.columns)-set(pm.columns))
  # print (set(pm.columns)-set(pt.columns))
  # print (set(prsellt))
  # print (set(prsellm))
  # print (set(prsellt.columns)-set(prsellm.columns))
  # print (set(prsellm.columns)-set(prsellt.columns))
  # print (set(bt.columns))
  # print (set(bm.columns))
  # print (set(bt.columns)-set(bm.columns))
  # print (set(bm.columns)-set(bt.columns))
  # print (set(ht.columns).__len__())
  # print (set(hm.columns).__len__())
  print (set(ht)-set(dc))
  # print ( set(ht.columns))
  # print (set(hm.columns))
  # print (set(ht.columns)-set(hm.columns))
  # print (set(hm.columns)-set(ht.columns))
