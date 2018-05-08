# coding=utf-8
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    
    sc = SparkSession.builder.appName("master").getOrCreate()
    ctx = SQLContext(sc)
    jdbcDf=ctx.read.format("jdbc").options(url="jdbc:mysql://10.30.1.70:3307/spark_test?useUnicode=true&characterEncoding=utf-8",
                                           driver="com.mysql.jdbc.Driver",
                                           dbtable="(SELECT * FROM ProjectInfoItem WHERE City='合肥')tmp",
                                           user="root",
                                           password="gh001").load()
    jdbcDf.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://10.30.1.70:3307/spark_mirror?useUnicode=true&characterEncoding=utf-8") \
        .option("dbtable", "testhaha") \
        .option("user", "root") \
        .option("password", "gh001") \
        .save()

    # jdbcDf=ctx.read.format("jdbc").options(url="jdbc:mysql://10.30.1.70:3307/spark_test",
    #                                        driver="com.mysql.jdbc.Driver",
    #                                        dbtable="(SELECT * FROM ProjectInfoItem WHERE City='合肥')",
    #                                        user="root",
    #                                        password="gh001").load()
    print(jdbcDf.printSchema())
    print (jdbcDf.show())
