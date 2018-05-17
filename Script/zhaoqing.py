# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql.functions import lit, col, collect_list
from pyspark.sql import Row, SparkSession
from SparkETLCore.Utils.Var import ENGINE, MIRROR_ENGINE, NiceDict
from SparkETLCore.Utils.Var import PROJECT_FIELDS, BUILDING_FIELDS, PRESELL_FIELDS,PERMIT_FIELDS, HOUSE_FIELDS
from SparkETLCore.CityCore.Zhaoqing import ProjectCore, BuildingCore, PresellCore, HouseCoreNew


def kwarguments(tableName, city, groupKey=None, db='spark_test'):
    if groupKey:
        dbtable = '(SELECT * FROM(SELECT * FROM {tableName} WHERE City="{city}" ORDER BY RecordTime DESC) AS col ' \
                  'Group BY {groupKey}) {tableName}'.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE City="{city}") {tableName}'.format(
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
    try:
        outdf = df.toDF().select(fields)

        if tableName == 'house_info_zhaoqing':
            supply_df = outdf.filter((outdf.HouseState == '可售') & (outdf.HouseStateLatest != '可售'))
            # supply_df = supply_df.withColumn("State", lit('明确供应'))
            write(supply_df, 'supply_case_zhaoqing')

            quit_df = outdf.filter((outdf.HouseState == '可售') & (outdf.HouseStateLatest == '已售'))
            # quit_df = quit_df.withColumn("State", lit('明确退房'))
            write(quit_df, 'quit_case_zhaoqing')

            deal_df = outdf.filter((outdf.HouseStateLatest == '可售') & (outdf.HouseState == '已售'))
            # deal_df = deal_df.withColumn("DealType", lit('明确成交'))
            write(deal_df, 'deal_case_zhaoqing')

            deal_df2 = outdf.filter((outdf.HouseStateLatest == '') & (outdf.HouseState == '已售'))
            # deal_df2 = deal_df2.withColumn("DealType", lit('明确成交'))
            write(deal_df2, 'deal_case_zhaoqing')

        write(outdf, tableName)
    except ValueError as e:
        import traceback
        traceback.print_exc()
    return df


appName = 'zhaoqing'
spark = SparkSession \
    .builder \
    .appName(appName) \
    .config('spark.cores.max', 4) \
    .config('spark.sql.execution.arrow.enabled', "true") \
    .config("spark.sql.codegen", "true") \
    .config("spark.local.dir", "~/.tmp") \
    .getOrCreate()

projectArgs = kwarguments('ProjectInfoItem', '肇庆')
projectDF = spark.read \
    .format("jdbc") \
    .options(**projectArgs) \
    .load() \
    .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('buildinginfoitem', '肇庆')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("buildinginfoitem")

houseArgs = kwarguments('houseinfoitem', '肇庆')
houseDF = spark.read \
    .format("jdbc") \
    .options(**houseArgs) \
    .load() \
    .fillna("")
houseDF.createOrReplaceTempView("houseinfoitem")

presellArgs = kwarguments('presellinfoitem', '肇庆')
presellDF = spark.read \
    .format("jdbc") \
    .options(**presellArgs) \
    .load() \
    .fillna("")
presellDF.createOrReplaceTempView("presellinfoitem")


def projectETL(pjDF=projectDF):
    presellBuildingDF = spark.sql('''
        select 
         ProjectUUID,
         concat_ws('@#$', collect_list(distinct PresalePermitNumber)) as PresalePermitNumber,
         concat_ws('@#$', collect_list(distinct LssueDate)) as LssueDate
         from (select p.PresalePermitNumber as PresalePermitNumber,p.LssueDate as LssueDate,b.ProjectUUID as ProjectUUID
         from buildinginfoitem b left join presellinfoitem p on p.PresalePermitNumber = b.PresalePermitNumber 
         where p.PresalePermitNumber != '') t GROUP BY ProjectUUID
    ''')

    projectHouseDF = spark.sql('''
        select ProjectUUID, 
        count(distinct HouseID) as HousingCount,
        concat_ws('@#$', collect_list(distinct HouseUseType)) as HouseUseType 
        from houseinfoitem group by ProjectUUID
        ''')

    projectHouseBuildingCountDF = spark.sql('''
        select ProjectUUID, count(distinct BuildingName) as HouseBuildingCount 
        from buildinginfoitem group by ProjectUUID
        ''')

    dropColumn = ['HousingCount', 'HouseUseType', 'LssueDate',
                  'PresalePermitNumber', 'HouseBuildingCount']
    pjDF = pjDF.drop(*dropColumn).dropDuplicates()
    print(pjDF.count())
    preProjectDF = pjDF.join(projectHouseDF, 'ProjectUUID', 'left') \
        .join(projectHouseBuildingCountDF, 'ProjectUUID', 'left') \
        .join(presellBuildingDF, 'ProjectUUID', 'left') \
        .select([lambda x: x not in dropColumn, pjDF.columns][1]
                + [projectHouseDF.HousingCount,
                   projectHouseDF.HouseUseType,
                   presellBuildingDF.PresalePermitNumber,
                   presellBuildingDF.LssueDate,
                   projectHouseBuildingCountDF.HouseBuildingCount]) \
        .dropDuplicates()
    # print(preProjectDF.count())
    groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                PROJECT_FIELDS, 'project_info_zhaoqing')

    return 0

def presellETL(preDF = presellDF):
    dropColumn = ['ProjectName']
    df = preDF.join(buildingDF,'PresalePermitNumber','left')\
                .select([lambda x: x not in dropColumn, preDF.columns][1]
                        +[buildingDF.ProjectName])
    groupedWork(df, PresellCore.METHODS, PresellCore,
                PRESELL_FIELDS, 'presell_info_zhaoqing')

def buildingETL(buildDF=buildingDF):
    buildingProjectDF = spark.sql('''
        select ProjectUUID,
        RealEstateProjectId,
        max(ProjectAddress) as Address  
        from ProjectInfoItem 
        group by ProjectUUID
        ''')
    buildingHouseDF = spark.sql('''
        select BuildingUUID,
        count(distinct HouseID) as HousingCount,
        concat_ws('@#$',collect_list(distinct FloorName)) as FloorName,
        concat_ws('@#$', collect_list(distinct BuildingStructure)) as BuildingStructure,
        concat_ws('@#$',collect_list(distinct MeasuredBuildingArea)) as MeasuredBuildingArea
        from houseinfoitem 
        group by BuildingUUID
        ''')

    dropColumn = ['RealEstateProjectId', 'Address', 'Floors', 'BuildingStructure', 'MeasuredBuildingArea']
    buildDF = buildDF.drop(*dropColumn).dropDuplicates()

    bDF = buildDF.join(buildingProjectDF, 'ProjectUUID', 'left') \
        .join(buildingHouseDF, 'BuildingUUID', 'left') \
        .select([lambda x: x not in dropColumn, buildDF.columns][1]
                + [buildingProjectDF.RealEstateProjectId,
                   buildingProjectDF.Address,
                   buildingHouseDF.ActualFloor,
                   buildingHouseDF.BuildingStructure,
                   buildingHouseDF.MeasuredBuildingArea]) \
        .dropDuplicates()

    groupedWork(bDF, BuildingCore.METHODS, BuildingCore,
                BUILDING_FIELDS, 'building_info_zhaoqing')


def houseETL(hDF=houseDF):
    dropColumn = ['ProjectID', 'RealEstateProjectID', 'BuildingID', 'DistrictName', 'RegionName',
                  'BuildingAveragePrice','PresalePermitNumber']
    hDF = hDF.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(
        ['ProjectUUID', 'ProjectID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName', 'RegionName'])
    b_df = buildingDF.select(['BuildingUUID', 'BuildingID', 'BuildingAveragePrice','PresalePermitNumber'])
    floorDF = spark.sql('''
        select 
        BuildingUUID,
         count(distinct FloorName) as Floors
         from houseinfoitem GROUP BY BuildingUUID
    ''')

    df = hDF.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .join(floorDF,'BuildingUUID')\
        .select([lambda x: x not in dropColumn, houseDF.columns][1]
                + [p_df.ProjectID,
                   p_df.RealEstateProjectID,
                   p_df.DistrictName,
                   p_df.ProjectAddress,
                   p_df.RegionName,
                   b_df.BuildingID,
                   b_df.BuildingAveragePrice,
                   b_df.PresalePermitNumber,
                floorDF.Floors]) \
        .dropDuplicates()
    groupedWork(df, HouseCoreNew.METHODS, HouseCoreNew,
                HOUSE_FIELDS, 'house_info_zhaoqing')

# if __name__ == "__main__":
#     projectETL(projectDF)
