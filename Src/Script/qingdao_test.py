# coding: utf-8
# @Date    : 2018-04-17 15:38:40

from __future__ import division
import sys

from sqlalchemy import create_engine

reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pandas as pd

sys.path.append('/home/chiufung/AwesomeSparkETL/Src/SparkETLCore')
sys.path.append('/home/junhui/workspace/AwesomeSparkETL/Src/SparkETLCore')

from SparkETLCore.Utils.Var import ENGINE, MIRROR_ENGINE
from SparkETLCore.CityCore.Qingdao import ProjectCore, PresellCore, BuildingCore, HouseCore, DealCaseCore, QuitCaseCore,SupplyCaseCore

def getData(cls, data):
	for method in cls.METHODS:
		func = getattr(cls, method)
		data = func(data)
	return data


if __name__ == '__main__':
	search_date = str(datetime.datetime.now().date())
	search_date = '2018-04-24'
	search_next_date = '2018-04-25'

	spark = SparkSession.builder.appName('sparkETL').getOrCreate()
	project_df = pd.read_sql(
		"select * from (select * from ProjectInfoItem where City = '{city}' and RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' order by "\
		"RecordTime desc ) as t GROUP BY ProjectID limit 5".format(
			city='青岛'.decode(), search_date=search_date,search_next_date=search_next_date), ENGINE).fillna('')
	if not project_df.empty:
		spark_df = spark.createDataFrame(project_df)
		del project_df
		res = spark_df.rdd.map(lambda r: getData(ProjectCore, r)).collect()
		rdd = spark.sparkContext.parallelize(res)
		pdf = rdd.toDF().toPandas()
		pdf.to_sql('project_info_qingdao', con=MIRROR_ENGINE, if_exists='append',index=False)


	presell_df = pd.read_sql("SELECT * FROM (SELECT * FROM PresellInfoItem WHERE City = '{city}'  AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' ORDER BY RecordTime DESC ) AS tb GROUP BY PresalePermitNumber ".format(
			city='青岛'.decode(), search_date=search_date,search_next_date=search_next_date), ENGINE).fillna('')
	if not presell_df.empty:
		spark_presell_df = spark.createDataFrame(presell_df)
		presell_res = spark_presell_df.rdd.map(lambda r:getData(PresellCore,r)).collect()
		presell_rdd = spark.sparkContext.parallelize(presell_res)
		presell_rdd.toDF().toPandas().to_sql('permit_info_qingdao',con=MIRROR_ENGINE,if_exists='append',index=False)

	
	building_df = pd.read_sql("SELECT * FROM (SELECT * FROM BuildingInfoItem WHERE City = '{city}'  AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' ORDER BY RecordTime DESC ) AS tb GROUP BY BuildingUUID ".format(
			city='青岛'.decode(), search_date=search_date,search_next_date=search_next_date), ENGINE).fillna('')
	if not building_df.empty:
		spark_building_df = spark.createDataFrame(building_df)
		building_res = spark_building_df.rdd.map(lambda r:getData(BuildingCore,r)).collect()
		building_rdd = spark.sparkContext.parallelize(building_res)
		building_rdd.toDF().toPandas().to_sql('building_info_qingdao',con = MIRROR_ENGINE,if_exists='append',index=False)

	house_df = pd.read_sql("SELECT * FROM (SELECT * FROM HouseInfoItem WHERE City = '{city}'  AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' ORDER BY RecordTime DESC ) AS tb GROUP BY HouseUUID ".format(
			city='青岛'.decode(), search_date=search_date,search_next_date=search_next_date), ENGINE).fillna('')
	if not house_df.empty:
		spark_house_df = spark.createDataFrame(house_df)
		house_res = spark_house_df.rdd.map(lambda r:getData(HouseCore,r)).collect()
		house_rdd = spark.sparkContext.parallelize(house_res)
		house_rdd.toDF().toPandas().to_sql('house_info_qingdao',con = MIRROR_ENGINE,if_exists='append',index=False)
	else:
		print(u'houseinfoitem没有数据')

	deal_df = pd.read_sql("SELECT * FROM HouseInfoItem WHERE City = '{city}'  AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' AND find_in_set('已签约',HouseState) AND HouseStateLatest = '可售' ORDER BY RecordTime DESC".format(city='青岛',
	search_date=search_date,search_next_date=search_next_date).decode('utf-8'),ENGINE).fillna('')
	if not deal_df.empty:
		spark_deal_df = spark.createDataFrame(deal_df)
		spark_deal_res = spark_deal_df.rdd.map(lambda r:getData(DealCaseCore,r)).collect()
		spark_deal_rdd = spark.sparkContext.parallelize(spark_deal_res)
		spark_deal_rdd.toDF().toPandas().to_sql('deal_case_qingdao',con=MIRROR_ENGINE,if_exists='append',index=False)

	quit_df = pd.read_sql(
		"SELECT * FROM HouseInfoItem WHERE City = '{city}'  AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' AND HouseState = '可售' AND "
		"find_in_set('已签约', HouseStateLatest)  ORDER BY RecordTime DESC".format(
			city='青岛', search_date=search_date,search_next_date=search_next_date).decode('utf-8'), ENGINE).fillna('')
	if not quit_df.empty:
		spark_quit_df = spark.createDataFrame(quit_df)
		spark_quitl_res = spark_quit_df.rdd.map(lambda r: getData(QuitCaseCore, r)).collect()
		spark_quit_rdd = spark.sparkContext.parallelize(spark_quitl_res)
		spark_quit_rdd.toDF().toPandas().to_sql('quit_case_qingdao', con=MIRROR_ENGINE, if_exists='append', index=False)

	supply_df = pd.read_sql(
		"SELECT * FROM HouseInfoItem WHERE City = '{city}' AND RecordTime >'{search_date}' AND RecordTime < '{search_next_date}' AND HouseState = '可售' AND HouseStateLatest != '可售' " \
		.format(city='青岛', search_date=search_date,search_next_date=search_next_date).decode(), ENGINE).fillna('')
	if not supply_df.empty:
		spark_supply_df = spark.createDataFrame(supply_df)
		spark_supply_res = spark_supply_df.rdd.map(lambda r: getData(SupplyCaseCore, r)).collect()
		spark_supply_rdd = spark.sparkContext.parallelize(spark_supply_res)
		spark_supply_rdd.toDF().toPandas().to_sql('supply_case_qingdao', con=MIRROR_ENGINE, if_exists='append', index=False)
