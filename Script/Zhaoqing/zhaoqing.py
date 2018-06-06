# coding=utf-8
import sys
import datetime
from pyspark.sql import Row, SparkSession
from SparkETLCore.Utils.Var import NiceDict
from SparkETLCore.Utils.Var import PROJECT_FIELDS, BUILDING_FIELDS, PRESELL_FIELDS, HOUSE_FIELDS, DEAL_FIELDS, SUPPLY_FIELDS, QUIT_FIELDS
from SparkETLCore.CityCore.Zhaoqing import ProjectCore, BuildingCore, PresellCore, HouseCore, DealCaseCore, SupplyCore, QuitCaseCore


def kwarguments(tableName, city, groupKey=None, db='naive'):
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


def groupedWork(data, methods, target, fields, tableName, distinctKey=None):
    res = None
    df = data
    df = df.rdd.repartition(1000).map(lambda r: cleanFields(
        r, methods, target, fields)).toDF().select(fields)
    argsDict = {'url': "jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
                'driver': "com.mysql.jdbc.Driver",
                'dbtable': tableName,
                'user': "root",
                'password': "yunfangdata"}
    try:
        df = df.unionByName(spark.read
                            .format("jdbc")
                            .options(**argsDict)
                            .load()
                            .fillna(""))
    except Exception as e:
        import traceback
        traceback.print_exc()
    if distinctKey:
        df = df.dropDuplicates(distinctKey)
    res = df.write.format("jdbc") \
        .options(**argsDict) \
        .mode("overwrite") \
        .save()
    return res


city = 'zhaoqing'
if len(sys.argv) < 2:
    appName = 'test'
else:
    appName = sys.argv[1]
spark = SparkSession\
    .builder\
    .appName('_'.join([city, appName]))\
    .config("spark.cores.max", 4)\
    .config('spark.sql.execution.arrow.enabled', "true") \
    .config("spark.sql.codegen", "true") \
    .getOrCreate()

projectArgs = kwarguments('projectinfoitem', '肇庆', 'ProjectID')
projectDF = spark.read \
    .format("jdbc") \
    .options(**projectArgs) \
    .load() \
    .fillna("")
projectDF.createOrReplaceTempView("ProjectInfoItem")

buildingArgs = kwarguments('buildinginfoitem', '肇庆', 'BuildingID')
buildingDF = spark.read \
    .format("jdbc") \
    .options(**buildingArgs) \
    .load() \
    .fillna("")
buildingDF.createOrReplaceTempView("buildinginfoitem")

houseArgs = kwarguments('houseinfoitem', '肇庆', 'HouseID')
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

    preProjectDF = pjDF.join(projectHouseDF, 'ProjectUUID', 'left') \
        .join(projectHouseBuildingCountDF, 'ProjectUUID', 'left') \
        .join(presellBuildingDF, 'ProjectUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, pjDF.columns))
                + [projectHouseDF.HousingCount,
                   projectHouseDF.HouseUseType,
                   presellBuildingDF.PresalePermitNumber,
                   presellBuildingDF.LssueDate,
                   projectHouseBuildingCountDF.HouseBuildingCount
                   ]) \
        .dropDuplicates()\
        .fillna('')
    groupedWork(preProjectDF, ProjectCore.METHODS, ProjectCore,
                PROJECT_FIELDS, 'project_info_zhaoqing', ['ProjectID'])

    return 0


def presellETL(preDF=presellDF):
    dropColumn = ['ProjectName']
    preDF = preDF.drop(*dropColumn).dropDuplicates()
    b_df = buildingDF.select(['ProjectName', 'PresalePermitNumber'])

    df = preDF.join(b_df, 'PresalePermitNumber', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, preDF.columns))
                + [b_df.ProjectName]) \
        .dropDuplicates() \
        .fillna('')
    groupedWork(df, PresellCore.METHODS, PresellCore,
                PRESELL_FIELDS, 'presell_info_zhaoqing', ['PresalePermitNumber'])


def buildingETL(buildDF=buildingDF):
    buildingProjectDF = spark.sql('''
        select ProjectUUID,
        first(RealEstateProjectID) as RealEstateProjectID,
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

    dropColumn = ['RealEstateProjectID', 'Address', 'Floors',
                  'HousingCount', 'BuildingStructure', 'MeasuredBuildingArea']
    buildDF = buildDF.drop(*dropColumn).dropDuplicates()

    bDF = buildDF.join(buildingProjectDF, 'ProjectUUID', 'left') \
        .join(buildingHouseDF, 'BuildingUUID', 'left') \
        .select(list(filter(lambda x: x not in dropColumn, buildDF.columns))
                + [buildingProjectDF.RealEstateProjectID,
                   buildingProjectDF.Address,
                   buildingHouseDF.HousingCount,
                   buildingHouseDF.FloorName,
                   buildingHouseDF.BuildingStructure,
                   buildingHouseDF.MeasuredBuildingArea]) \
        .dropDuplicates()

    groupedWork(bDF, BuildingCore.METHODS, BuildingCore,
                BUILDING_FIELDS, 'building_info_zhaoqing', ['BuildingID'])


def houseETL(hDF=houseDF):
    dropColumn = ['ProjectID', 'RealEstateProjectID', 'BuildingID', 'DistrictName', 'RegionName', 'Address',
                  'BuildingAveragePrice']
    hDF = hDF.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(
        ['ProjectUUID', 'ProjectID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName', 'RegionName']) \
        .withColumnRenamed('ProjectAddress', 'Address')
    b_df = buildingDF.select(
        ['BuildingUUID', 'BuildingID', 'BuildingAveragePrice'])

    df = hDF.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .select(list(filter(lambda x: x not in dropColumn, houseDF.columns))
                + [p_df.ProjectID,
                   p_df.RealEstateProjectID,
                   p_df.DistrictName,
                   p_df.Address,
                   p_df.RegionName,
                   b_df.BuildingID,
                   b_df.BuildingAveragePrice]) \
        .dropDuplicates()
    groupedWork(df, HouseCore.METHODS, HouseCore,
                HOUSE_FIELDS, 'house_info_zhaoqing', ['HouseID'])


def supplyETL(df=houseDF):
    supply_df = df.filter("RecordTime>='%s'" % str(datetime.datetime.now() - datetime.timedelta(days=7))).filter((df.HouseState.isin(['期房待售', '现房待售'])) & (
        ~ (df.HouseStateLatest.like('待售'))))

    dropColumn = ['ProjectID', 'RealEstateProjectID', 'BuildingID', 'DistrictName', 'RegionName', 'Address',
                  'BuildingAveragePrice', 'PresalePermitNumber']
    supply_df = supply_df.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(
        ['ProjectUUID', 'ProjectID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName', 'RegionName'])\
        .withColumnRenamed('ProjectAddress', 'Address')
    b_df = buildingDF.select(
        ['BuildingUUID', 'BuildingID', 'BuildingAveragePrice', 'PresalePermitNumber'])
    floorDF = spark.sql('''
                select 
                BuildingUUID,
                 count(distinct FloorName) as Floors
                 from houseinfoitem GROUP BY BuildingUUID
            ''')

    supply_df = supply_df.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .join(floorDF, 'BuildingUUID') \
        .select(list(filter(lambda x: x not in dropColumn, houseDF.columns))
                + [p_df.ProjectID,
                   p_df.RealEstateProjectID,
                   p_df.DistrictName,
                   p_df.Address,
                   p_df.RegionName,
                   b_df.BuildingID,
                   b_df.BuildingAveragePrice,
                   b_df.PresalePermitNumber,
                   floorDF.Floors]) \
        .dropDuplicates()
    groupedWork(supply_df, SupplyCore.METHODS, SupplyCore,
                SUPPLY_FIELDS, 'supply_case_zhaoqing', ['RecordTime', 'HouseID'])


def dealETL(df=houseDF):
    deal_df = df.filter("RecordTime>='%s'" % str(datetime.datetime.now() - datetime.timedelta(days=7))).filter((df.HouseStateLatest.isin(['期房待售', '现房待售', ''])) & (
        df.HouseState.isin(['已签约', '已备案', '已登记'])))

    dropColumn = ['ProjectID', 'RealEstateProjectID', 'BuildingID', 'DistrictName', 'RegionName', 'Address',
                  'BuildingAveragePrice', 'PresalePermitNumber']
    deal_df = deal_df.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(
        ['ProjectUUID', 'ProjectID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName', 'RegionName'])\
        .withColumnRenamed('ProjectAddress', 'Address')
    b_df = buildingDF.select(
        ['BuildingUUID', 'BuildingID', 'BuildingAveragePrice', 'PresalePermitNumber'])
    floorDF = spark.sql('''
                select 
                BuildingUUID,
                 count(distinct FloorName) as Floors
                 from houseinfoitem GROUP BY BuildingUUID
            ''')

    deal_df = deal_df.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .join(floorDF, 'BuildingUUID') \
        .select(list(filter(lambda x: x not in dropColumn, houseDF.columns))
                + [p_df.ProjectID,
                   p_df.RealEstateProjectID,
                   p_df.DistrictName,
                   p_df.Address,
                   p_df.RegionName,
                   b_df.BuildingID,
                   b_df.BuildingAveragePrice,
                   b_df.PresalePermitNumber,
                   floorDF.Floors]) \
        .dropDuplicates()
    groupedWork(deal_df, DealCaseCore.METHODS, DealCaseCore,
                DEAL_FIELDS, 'deal_case_zhaoqing', ['RecordTime', 'HouseID'])


def quitETL(df=houseDF):
    quit_df = df.filter("RecordTime>='%s'" % str(datetime.datetime.now() - datetime.timedelta(days=7))).filter((df.HouseState.isin(['期房待售', '现房待售'])) & (
        df.HouseStateLatest.isin(['已签约', '已备案', '已登记'])))

    dropColumn = ['ProjectID', 'RealEstateProjectID', 'BuildingID', 'DistrictName', 'RegionName', 'Address',
                  'BuildingAveragePrice', 'PresalePermitNumber']
    quit_df = quit_df.drop(*dropColumn).dropDuplicates()
    p_df = projectDF.select(
        ['ProjectUUID', 'ProjectID', 'RealEstateProjectID', 'ProjectAddress', 'DistrictName', 'RegionName'])\
        .withColumnRenamed('ProjectAddress', 'Address')
    b_df = buildingDF.select(
        ['BuildingUUID', 'BuildingID', 'BuildingAveragePrice', 'PresalePermitNumber'])
    floorDF = spark.sql('''
                select 
                BuildingUUID,
                 count(distinct FloorName) as Floors
                 from houseinfoitem GROUP BY BuildingUUID
            ''')

    quit_df = quit_df.join(p_df, 'ProjectUUID') \
        .join(b_df, 'BuildingUUID') \
        .join(floorDF, 'BuildingUUID') \
        .select(list(filter(lambda x: x not in dropColumn, houseDF.columns))
                + [p_df.ProjectID,
                   p_df.RealEstateProjectID,
                   p_df.DistrictName,
                   p_df.Address,
                   p_df.RegionName,
                   b_df.BuildingID,
                   b_df.BuildingAveragePrice,
                   b_df.PresalePermitNumber,
                   floorDF.Floors]) \
        .dropDuplicates()
    groupedWork(quit_df, QuitCaseCore.METHODS, QuitCaseCore,
                QUIT_FIELDS, 'quit_case_zhaoqing', ['RecordTime', 'HouseID'])


def main():
    methodsDict = {'projectETL': projectETL,
                   'buildingETL': buildingETL,
                   'presellETL': presellETL,
                   'houseETL': houseETL,
                   'dealETL': dealETL,
                   'supplyETL': supplyETL,
                   'quitETL': quitETL,
                   }
    if len(sys.argv) < 2:
        return 0
    else:
        methodInstance = methodsDict.get(sys.argv[1])
        if methodInstance:
            methodInstance()
    return 0


if __name__ == "__main__":
    main()
