# coding=utf-8
import json
import pandas as pd
from pyspark.sql import SparkSession,SQLContext
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer
from pyspark.sql import Row
import ProjectCore
import HouseCore
import BuildingCore
import QuitCaseCore
import SupplyCaseCore
import DealCaseCore
from pyspark import SparkContext
from pyspark import SparkConf

ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')

sconf = SparkConf()
sconf.set('spark.cores.max', 4)
sc = SparkContext(appName='changzhou', conf=sconf)
ctx = SQLContext(sc)

def getData(cls, data):
    for method in cls.METHODS:
        func = getattr(cls, method)
        data = func(data)
    return data
def mapping_df_types(df):
    dtypedict = {}
    ignorecolumns = {'ExtraJson', 'Content'}
    for i, j in zip(df.columns, df.dtypes):
        if i in ignorecolumns:
            continue
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
    return dtypedict

def write_data2sql(pdf,tablename,METHODS):
    dtypedict = mapping_df_types(pdf)
    methods = [x.lower() for x in METHODS]
    housecols = [x for x in pdf.columns]
    usecols = [x for x in housecols if x.lower() in methods]
    pdf[usecols].to_sql(tablename, MIRROR_ENGINE, index=False,
                       if_exists='append', dtype=dtypedict, chunksize=100)
    return len(pdf)

def runFunc(func, tosqlname):
    # data = pd.read_sql(sql, ENGINE)
    jdbcDF = ctx.read.format("jdbc").options(
        url='jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf8',
        driver="com.mysql.jdbc.Driver", dbtable="HouseInfoItem", user="root", password="gh001").load()

    # SparkSession.builder.appName(func.__name__)
    # spark = SparkSession.builder.appName(func.__name__).getOrCreate()
    # dataDF = spark.createDataFrame(data)
    jdbcDF =jdbcDF.where((jdbcDF.City==u'常州') & (jdbcDF.HouseStateLatest==u'已备案'))
    res = jdbcDF.rdd.map(lambda r: getData(func, r))
    result = res.map(lambda x:lambda x: write_data2sql(x,tosqlname,func.METHODS))
    result.pprint()
    # res = jdbcDF.rdd.map(lambda r: getData(func, r)).collect()
    # rdd = spark.sparkContext.parallelize(res)
    # pdf = rdd.toDF().toPandas()
    # methods = [x.lower() for x in func.METHODS]
    # housecols = [x for x in pdf.columns]
    # usecols = [x for x in housecols if x.lower() in methods]
    # pdf[usecols].to_sql(tosqlname, MIRROR_ENGINE,
    #                     if_exists='append', index=False)

#
# quit_sql = u"select * from HouseInfoItem where City='常州' and HouseStateLatest='已备案' "
# runFunc(QuitCaseCore, 'quit_case_changzhou')
# supply_sql = u"select * from HouseInfoItem where City='常州' and (HouseStateLatest='未备案' or HouseStateLatest='') and HouseState='未备案'"
# runFunc(SupplyCaseCore, supply_sql, 'supply_case_changzhou')
# deal_sql = u"select * from HouseInfoItem where City='常州' and HouseStateLatest='未备案' and HouseState='已备案'"
# runFunc(DealCaseCore, deal_sql, 'deal_case_changzhou')

# select_sql = "select * from HouseInfoItem where City='常州'"
# data = pd.read_sql(select_sql, ENGINE)
# SparkSession.builder.appName('spark')
# spark = SparkSession.builder.appName('spark').getOrCreate()
# dataDF = spark.createDataFrame(data)
# res = dataDF.rdd.map(lambda r: getData(HouseCore, r)).collect()
# rdd = spark.sparkContext.parallelize(res)
# pdf = rdd.toDF().toPandas()
# pdf.to_sql('house_info_changzhou', MIRROR_ENGINE,
#            if_exists='replace', index=False)
