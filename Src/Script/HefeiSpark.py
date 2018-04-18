from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

def cleanProject(sc):
    url = "jdbc:mysql://localhost:3306/citymap_pgy_xuzhou"
    jdbcDf=sc.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/citymap_pgy_xuzhou",
                                           driver="com.mysql.jdbc.Driver",
                                           dbtable="(SELECT * FROM apartment_product) tmp",user="root",
                                           password="123456").load()
    print(jdbcDf.printSchema())
    print (jdbcDf.show())

if __name__ == '__main__':
    sc = SparkSession.builder.appName("master").getOrCreate()
    cleanProject(sc,host,user,pwd)