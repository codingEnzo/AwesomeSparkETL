# coding:utf-8
import sys
import datetime
from pyspark.sql import Row
from pyspark.sql import SparkSession
from SparkETLCore.CityCore.Hefei import ProjectCore, BuildingCore, HouseCore,\
    DealCaseCore, SupplyCaseCore, QuitCaseCore
from SparkETLCore.Utils.Var import PROJECT_FIELDS, BUILDING_FIELDS,\
    HOUSE_FIELDS, DEAL_FIELDS, SUPPLY_FIELDS, QUIT_FIELDS, NiceDict


def kwarguments(sc, tableName=None, city=None, groupKey=None, query=None, db='naive'):
    if groupKey:
        dbtable = '''(SELECT * FROM
                     (SELECT * FROM {tableName}
                     WHERE city="{city}"
                     ORDER BY RecordTime DESC) AS col 
                     Group BY {groupKey}) {tableName}'''.format(
            city=city, tableName=tableName, groupKey=groupKey)
    else:
        dbtable = '(SELECT * FROM {tableName} WHERE city="{city}") {tableName}'.format(
            city=city, tableName=tableName)
    options = {
        'url': "jdbc:mysql://10.30.1.7:3306/{}?useUnicode=true&characterEncoding=utf8".format(db),
        "user": "root", "password": "yunfangdata",
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": query or dbtable}
    df = sc.read.format("jdbc").options(**options).load().fillna('')
    df.createOrReplaceTempView(tableName)
    return df


def cleanFields(row, methods, target, fields):
    row = row.asDict()
    row = NiceDict(dictionary=row, target=fields)
    for i, method in enumerate(methods):
        row = getattr(target, method)(row)
    row = Row(**row)
    return row


def groupedWork(data, methods, target, fields, tableName, distinctKey=None):
    res = None
    maxRecordTime = "1970-01-01 00:00:00"
    df = data
    argsDictRead = {'url': "jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
                    'driver': "com.mysql.jdbc.Driver",
                    'dbtable': "(select max(RecordTime) as RecordTime from {tableName}) {tableName}".format(tableName=tableName),
                    'user': "root",
                    'password': "yunfangdata"}
    argsDictWrite = {'url': "jdbc:mysql://10.30.1.7:3306/achievement?useUnicode=true&characterEncoding=utf8",
                     'driver': "com.mysql.jdbc.Driver",
                     'dbtable': tableName,
                     'user': "root",
                     'password': "yunfangdata"}
    try:
        maxRecordTime = spark.read\
            .format("jdbc")\
            .options(**argsDictRead)\
            .load()\
            .fillna("")\
            .first().RecordTime
    except Exception:
        import traceback
        traceback.print_exc()
    df = df.filter("RecordTime>='%s'" % str(maxRecordTime))
    df = df.rdd.repartition(1000).map(lambda r: cleanFields(
        r, methods, target, fields)).toDF().select(fields)
    if distinctKey:
        df = df.dropDuplicates(distinctKey)
    res = df.write.format("jdbc") \
        .options(**argsDictWrite) \
        .mode("append") \
        .save()
    return res


sc = SparkSession.builder.appName("Hefei" + '_' + sys.argv[1])\
    .config('spark.cores.max', 6)\
    .config('spark.sql.execution.arrow.enabled', 'true')\
    .config('spark.sql.codegen', 'true').getOrCreate()
spark = sc


def projectETL(sc):
    dropColumn = ['HouseBuildingCount', 'PresalePermitNumber']
    pdf = sc.sql('SELECT * FROM ProjectInfoItem')
    bdf = sc.sql(
        '''SELECT ProjectUUID,
            count(distinct(BuildingName)) AS HouseBuildingCount,
            concat_ws('@#$', collect_list(distinct PresalePermitNumber)) AS PresalePermitNumber,
            concat_ws('@#$', collect_list(ExtraJson)) AS BuildingExtraJson
            FROM BuildingInfoItem GROUP BY ProjectUUID''')
    project, building = pdf.alias('project'), bdf.alias('building')
    project = project.join(building, 'ProjectUUID', 'left').select(
        list(filter(lambda x: x not in dropColumn, project.columns)) +
        [building.HouseBuildingCount,
         building.PresalePermitNumber,
         building.BuildingExtraJson])
    return groupedWork(project, ProjectCore.METHODS, ProjectCore,
                       PROJECT_FIELDS, 'project_info_hefei', ['ProjectID'])


def buildingETL(sc):
    dropColumn = ['Address', 'Floors']
    pdf = sc.sql(
        '''SELECT first(ProjectUUID) AS ProjectUUID,
            first(ProjectAddress) AS Address
            FROM ProjectInfoItem''')
    bdf = sc.sql('SELECT * From BuildingInfoItem')
    hdf = sc.sql(
        '''SELECT BuildingUUID AS BuildingUUID,
            count(distinct(FloorName)) AS Floors
            FROM HouseInfoItem GROUP BY BuildingUUID''')
    project, building, house = pdf.alias(
        'project'), bdf.alias('building'), hdf.alias('house')

    building = building.join(project, 'ProjectUUID', 'left')\
        .join(house, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, building.columns)) +
                [project.Address, house.Floors])
    # building.select(project.Address).show(30)
    return groupedWork(building, BuildingCore.METHODS, BuildingCore,
                       BUILDING_FIELDS, 'building_info_hefei', ['BuildingID'])


def houseETL(sc):
    dropColumn = ['DistrictName', 'Address', 'PresalePermitNumber']
    pdf = sc.sql(
        '''SELECT ProjectUUID,
            DistrictName AS DistrictName,
            ProjectAddress AS Address
            FROM ProjectInfoItem ''')
    bdf = sc.sql(
        '''SELECT BuildingUUID,
            PresalePermitNumber AS PresalePermitNumber 
            FROM BuildingInfoItem''')
    hdf = sc.sql('SELECT * FROM HouseInfoItem')
    project, building, house = pdf.alias(
        'project'), bdf.alias('building'), hdf.alias('house')
    house = house.join(project, 'ProjectUUID', 'left')\
        .join(building, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, house.columns)) +
                [project.DistrictName,
                 project.Address,
                 building.PresalePermitNumber])
    return groupedWork(house, HouseCore.METHODS, HouseCore,
                       HOUSE_FIELDS, 'house_info_hefei', ['HouseID'])


def dealCaseETL(sc):
    dropColumn = ['DistrictName', 'Address', 'PresalePermitNumber']
    pdf = sc.sql(
        '''SELECT ProjectUUID,
            DistrictName AS DistrictName,
            ProjectAddress AS Address
            FROM ProjectInfoItem ''')
    bdf = sc.sql(
        '''SELECT BuildingUUID,
            PresalePermitNumber AS PresalePermitNumber 
            FROM BuildingInfoItem''')
    hdf = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('已签约','已备案','已办产权','网签备案单') 
            AND HouseStateLatest in ('可售','抵押可售','摇号销售','现房销售')''')

    project, building, house = pdf.alias(
        'project'), bdf.alias('building'), hdf.alias('house')
    house = house.join(project, 'ProjectUUID', 'left')\
                 .join(building, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, house.columns)) +
                [project.DistrictName,
                 project.Address,
                 building.PresalePermitNumber])
    return groupedWork(house, DealCaseCore.METHODS, DealCaseCore,
                       DEAL_FIELDS, 'deal_case_hefei', ['RecordTime', 'HouseID'])


def supplyCaseETL(sc):
    dropColumn = ['DistrictName', 'Address', 'PresalePermitNumber']
    pdf = sc.sql(
        '''SELECT ProjectUUID,
            DistrictName AS DistrictName,
            ProjectAddress AS Address
            FROM ProjectInfoItem ''')
    bdf = sc.sql(
        '''SELECT BuildingUUID,
            PresalePermitNumber AS PresalePermitNumber 
            FROM BuildingInfoItem''')
    hdf = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售') 
            AND HouseStateLatest=''
            ''')
    project, building, house = pdf.alias(
        'project'), bdf.alias('building'), hdf.alias('house')
    house = house.join(project, 'ProjectUUID', 'left')\
                 .join(building, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, house.columns)) +
                [project.DistrictName,
                 project.Address,
                 building.PresalePermitNumber])
    return groupedWork(house, SupplyCaseCore.METHODS, SupplyCaseCore,
                       SUPPLY_FIELDS, 'supply_case_hefei', ['RecordTime', 'HouseID'])


def quitCaseETL(sc):
    dropColumn = ['DistrictName', 'Address', 'PresalePermitNumber']
    pdf = sc.sql(
        '''SELECT ProjectUUID,
            DistrictName AS DistrictName,
            ProjectAddress AS Address
            FROM ProjectInfoItem ''')
    bdf = sc.sql(
        '''SELECT BuildingUUID,
            PresalePermitNumber AS PresalePermitNumber 
            FROM BuildingInfoItem''')
    hdf = sc.sql(
        '''SELECT * FROM HouseInfoItem 
            WHERE HouseState in ('可售','抵押可售','摇号销售','现房销售')
            AND HouseStateLatest in ('已签约','已备案','已办产权','网签备案单') 
            ''')
    project, building, house = pdf.alias(
        'project'), bdf.alias('building'), hdf.alias('house')
    house = house.join(project, 'ProjectUUID', 'left')\
                 .join(building, 'BuildingUUID', 'left')\
        .select(list(filter(lambda x: x not in dropColumn, house.columns)) +
                [project.DistrictName,
                 project.Address,
                 building.PresalePermitNumber])
    return groupedWork(house, QuitCaseCore.METHODS, QuitCaseCore,
                       QUIT_FIELDS, 'quit_case_hefei', ['RecordTime', 'HouseID'])


def main():
    projectDF = kwarguments(sc=sc, tableName='ProjectInfoItem',
                            city='合肥', groupKey='ProjectID',
                            query=None)
    buildingDF = kwarguments(sc=sc, tableName='BuildingInfoItem',
                             city='合肥', groupKey='BuildingID',
                             query=None)
    houseDF = kwarguments(sc=sc, tableName='HouseInfoItem',
                          city='合肥', groupKey='HouseID',
                          query=None,)
    methodsDict = {'projectETL': projectETL,
                   'buildingETL': buildingETL,
                   'houseETL': houseETL,
                   'dealETL': dealCaseETL,
                   'supplyETL': supplyCaseETL,
                   'quitETL': quitCaseETL}
    if len(sys.argv) < 2:
        return 0
    else:
        methodInstance = methodsDict.get(sys.argv[1])
        if methodInstance:
            methodInstance(sc)
    return 0


if __name__ == '__main__':
    main()
