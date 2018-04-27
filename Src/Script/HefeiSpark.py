# coding:utf-8
import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from sqlalchemy import *
from pyspark.sql.types import *
sys.path.append(os.path.dirname(os.getcwd()))
from SparkETLCore.CityCore.Hefei import ProjectCore
from SparkETLCore.CityCore.Hefei import BuildingCore
from SparkETLCore.CityCore.Hefei import PermitCore
from SparkETLCore.CityCore.Hefei import HouseCore
from SparkETLCore.CityCore.Hefei import DealcaseCore
from SparkETLCore.Utils.Var import MIRROR_ENGINE
from SparkETLCore.Utils.Var import ENGINE


def projectClean(seaechtime='1970-01-01'):
    # schema = StructType([StructField(i,StringType(),True) for i in df.columns])
    print ('project script is going run')
    def enterCore(data):
        # print(data)
        data = ProjectCore.test(data)
        # for index,method in enumerate(ProjectCore.METHODS):
            # if method =='test':
            #     func = getattr(PermitCore,method)
            #     data = func(data)
        return data
        # return '123'
    ENGINE = create_engine(
        'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
    sc = SparkSession.builder.appName("master").getOrCreate()
    df = pd.read_sql("SELECT * FROM ProjectInfoItem WHERE City='合肥' and RecordTime>'{0}' LIMIT 1"\
                    .format(seaechtime).decode('utf-8'),con=ENGINE).fillna('')
    sdf = sc.createDataFrame(df)
    sdd =sdf.rdd
    iterList = sdd.map(lambda r:enterCore(r)).collect()
    cleansdd = sc.sparkContext.parallelize(iterList)
    cleansdf = cleansdd.toDF()
    cleanpdf = cleansdf.toPandas()
    cleanpdf.to_sql('project_info_hefei2',con=MIRROR_ENGINE,if_exists='append',index=False)  
    print ('project script has finished')

if __name__ == '__main__':
    print (pyspark.version)
    projectClean()
    # print (b[1])

    # print(sdd.take(1))

    # cleanProject(sc,host,user,pwd)
