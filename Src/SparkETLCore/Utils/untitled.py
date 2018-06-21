import pymongo
from sqlalchemy import create_engine
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
sc.stop()
spark = SparkSession.builder.appName('mongo').config(
    "spark.cores.max", 4).config('spark.local.dir', '.tmp').getOrCreate()
db = pymongo.MongoClient('10.30.1.12')['EsfBaseData']
col1 = db['projectInfo']
cur = col1.find()
ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
ridf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
    "uri", "mongodb://10.30.1.12:27017/EsfBaseData.roomInfo").load().drop('_id').dropDuplicates()
ridf = ridf.drop(*['city', 'roomNo', 'danyuan', 'floor', 'tab', 'uniqcode'])
a = set()
for i in cur:
    a.add(i.get('projcode'))
a = list(a)
skip = 5000
a_s = list(range(0, len(a), skip))
for ind in a_s:
    pdf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://10.30.1.12:27017/EsfBaseData.projectInfo").load(
    ).drop('_id').filter(col('projcode').isin(a[ind:ind + skip])).dropDuplicates().drop('tab')
    bdf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://10.30.1.12:27017/EsfBaseData.buildingInfo").load(
    ).drop('_id').filter(col('newcode').isin(a[ind:ind + skip])).dropDuplicates().drop(*['projname', 'city', 'tab', 'danyuan'])
    udf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://10.30.1.12:27017/EsfBaseData.unitInfo").load(
    ).drop('_id').filter(col('newcode').isin(a[ind:ind + skip])).dropDuplicates().drop(*['newcode', 'city', 'tab'])
    rdf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://10.30.1.12:27017/EsfBaseData.roomInfoBase").load(
    ).drop('_id').filter(col('newcode').isin(a[ind:ind + skip])).dropDuplicates().drop(*['newcode', 'city', 'dongid', 'tab'])
    df = pdf
    df = df.join(bdf, df.projcode == bdf.newcode, 'left')
    df = df.join(udf, 'dongid', 'left')
    df = df.join(rdf, df.danyuanid == rdf.danyuan, 'left')
    df = df.join(ridf, 'roomid', 'left')
    df = df.na.fill('').toPandas()
    df.to_sql('esf', ENGINE, if_exists='append')
