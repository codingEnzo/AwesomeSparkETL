# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql.functions import lit
from pyspark.sql import Row, SparkSession
from SparkETLCore.Utils.Var import ENGINE, MIRROR_ENGINE, NiceDict
from SparkETLCore.Utils.Var import PROJECT_FIELDS, BUILDING_FIELDS, PERMIT_FIELDS, HOUSE_FIELDS
from SparkETLCore.CityCore.Qingdao import ProjectCore, BuildingCore, PresellCore, HouseCore


def kwarguments(tableName, city, groupKey=None, db='spark_test'):
    if groupKey:
        dbtable = '(SELECT * FROM(SELECT * FROM {tableName} WHERE city="{city}" ORDER BY RecordTime DESC) AS col ' \
                  'Group BY {groupKey}) {tableName}'.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE city="{city}") {tableName}'.format(
            city=city, tableName=tableName)
    return {
        "url": "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8".format(db),
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": dbtable,
        "user": "root",
        "password": "yunfangdata"
    }


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    row = NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def write(df, tableName):
    df.write.format("jdbc") \
        .options(
        url="jdbc:mysql://10.30.1.7:3306/mirror?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true",
        driver="com.mysql.jdbc.Driver",
        dbtable=tableName,
        user="root",
        password="yunfangdata") \
        .mode("append") \
        .save()


def groupedWork(data, methods, target, fields, tableName):
    df = data
    df = df.rdd.map(lambda r: cleanFields(r, methods, target, fields))
    outdf = df.toDF().select(fields)

    if tableName == 'house_info_qingdao':
        supply_df = outdf.filter((outdf.HouseState == '可售') & (outdf.HouseStateLatest != '可售'))
        supply_df = supply_df.withColumn("State", lit('明确供应'))
        write(supply_df, 'supply_case_qingdao')

        quit_df = outdf.filter((outdf.HouseState == '可售') & (outdf.HouseStateLatest.rlike('已签约')))
        quit_df = quit_df.withColumn("State", lit('明确退房'))
        write(quit_df, 'quit_case_qingdao')

        deal_df = outdf.filter((outdf.HouseStateLatest == '可售') & (outdf.HouseState.rlike('已签约')))
        deal_df = deal_df.withColumn("DealType", lit('明确成交'))
        write(deal_df, 'deal_case_qingdao')

        deal_df2 = outdf.filter((outdf.HouseStateLatest == '') & (outdf.HouseState.rlike('已签约')))
        deal_df2 = deal_df2.withColumn("DealType", lit('明确成交'))
        write(deal_df2, 'deal_case_qingdao')

    write(outdf, tableName)
    return df


appName = 'qingdao'
spark = SparkSession\
    .builder\
    .appName(appName)\
    .config('spark.cores.max', 4)\
    .config('spark.sql.execution.arrow.enabled', "true")\
    .config("spark.sql.codegen", "true")\
    .config("spark.local.dir", "~/.tmp")\
    .getOrCreate()

projectArgs = kwarguments('ProjectInfoItem', '青岛')
projectDF = spark.read \
    .format("jdbc") \
    .options(**projectArgs) \
    .load() \
    .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('BuildingInfoItem', '青岛')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("BuildingInfoItem")

houseArgs = kwarguments('HouseInfoItem', '青岛')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("HouseInfoItem")

presellArgs = kwarguments('PresellInfoItem', '青岛')
presellDF = spark.read \
    .format("jdbc") \
    .options(**presellArgs) \
    .load() \
    .fillna("")
presellDF.createOrReplaceTempView("PresellInfoItem")


def projectETL(pjDF=projectDF):
    projectHouseDF = spark.sql('''
        select ProjectUUID, 
        count(distinct HouseID) as HousingCount,
        concat_ws('@#$', collect_list(distinct HouseUseType)) as HouseUseType 
        from HouseInfoItem group by ProjectUUID
        ''')

    projectPresaleDF = spark.sql('''
        select ProjectUUID, 
        concat_ws('@#$', collect_list(distinct PresalePermitNumber)) as PresalePermitNumber,
        concat_ws('@#$', collect_list(distinct LssueDate)) as LssueDate 
        from PresellInfoItem group by ProjectUUID
        ''')
    projectHouseBuildingCountDF = spark.sql('''
        select ProjectUUID, count(distinct BuildingName) as HouseBuildingCount 
        from BuildingInfoItem group by ProjectUUID
        ''')

    dropColumn = ['HousingCount', 'HouseUseType', 'LssueDate',
                  'PresalePermitNumber', 'HouseBuildingCount']
    pjDF = pjDF.drop(*dropColumn).dropDuplicates()
    print(pjDF.count())
    preProjectDF = pjDF.join(projectHouseDF, 'ProjectUUID', 'left') \
        .join(projectHouseBuildingCountDF, 'ProjectUUID', 'left') \
        .join(projectPresaleDF, 'ProjectUUID', 'left') \
        .select([lambda x: x not in dropColumn, pjDF.columns][1]
                + [projectHouseDF.HousingCount,
                   projectHouseDF.HouseUseType,
                   projectPresaleDF.PresalePermitNumber,
                   projectPresaleDF.LssueDate,
                   projectHouseBuildingCountDF.HouseBuildingCount]) \
        .dropDuplicates()
    print(preProjectDF.count())
    groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                PROJECT_FIELDS, 'project_info_qingdao')

    return 0


def buildingETL(buildDF=buildingDF):
    buildingProjectDF = spark.sql('''
        select ProjectUUID,
        max(ProjectAddress) as Address  
        from ProjectInfoItem 
        group by ProjectUUID
        ''')
    buildingHouseDF = spark.sql('''
        select BuildingUUID,
        concat_ws('@#$',collect_list(distinct ActualFloor)) as ActualFloor,
        concat_ws('@#$', collect_list(distinct BuildingStructure)) as BuildingStructure,
        concat_ws('@#$',collect_list(distinct MeasuredBuildingArea)) as MeasuredBuildingArea
        from HouseInfoItem 
        group by BuildingUUID
        ''')

    dropColumn = ['Address', 'Floors', 'BuildingStructure', 'MeasuredBuildingArea']
    buildDF = buildDF.drop(*dropColumn).dropDuplicates()

    bDF = buildDF.join(buildingProjectDF, 'ProjectUUID', 'left') \
        .join(buildingHouseDF, 'BuildingUUID', 'left') \
        .select([lambda x: x not in dropColumn, buildDF.columns][1]
                + [buildingProjectDF.Address,
                   buildingHouseDF.ActualFloor,
                   buildingHouseDF.BuildingStructure,
                   buildingHouseDF.MeasuredBuildingArea]) \
        .dropDuplicates()

    groupedWork(bDF, BuildingCore.METHODS, BuildingCore,
                BUILDING_FIELDS, 'building_info_qingdao')


def presellETL(preDF=presellDF):
    groupedWork(preDF, PresellCore.METHODS, PresellCore,
                PERMIT_FIELDS, 'permit_info_qingdao')


def houseETL(hDF=houseDF):
    dropColumn = ['RealEstateProjectID', 'BuildingID', 'DistrictName', 'BuildingAveragePrice']
    hDF = hDF.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(['ProjectUUID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName'])
    b_df = buildingDF.select(['BuildingUUID', 'BuildingID', 'BuildingAveragePrice'])

    df = hDF.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .select([lambda x: x not in dropColumn, houseDF.columns][1]
                + [
                    projectDF.RealEstateProjectID,
                    projectDF.DistrictName,
                    projectDF.ProjectAddress,
                    buildingDF.BuildingID,
                    buildingDF.BuildingAveragePrice]) \
        .dropDuplicates()
    groupedWork(df, HouseCore.METHODS, HouseCore,
                HOUSE_FIELDS, 'house_info_qingdao')

# if __name__ == "__main__":
#     projectETL(projectDF)
