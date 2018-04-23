# coding=utf-8
from __future__ import division
import sys
import datetime
import inspect
import pandas as pd
import numpy as np
import os 
sys.path.append(os.path.dirname(os.getcwd()))
from pyspark.sql import Row
from SparkETLCore.Utils import  Meth, Config,Var
nowtime = datetime.datetime.now() 

METHODS =['approvalPresaleAmount',
         'approvalPresaleArea',
         'approvalPresaleHouseAmount',
         'approvalPresaleHouseArea',
         'approvalPresalePosition',
         'builtFloorCount',
         'constructionFloorCount',
         'constructionTotalArea',
         'contacts',
         'earliestOpeningDate',
         'earliestStartDate',
         'groundArea',
         'houseSpread',
         'landUse',
         'latestDeliversHouseDate',
         'lssueDate',
         'lssuingAuthority',
         'periodsCount',
         'presaleBuildingAmount',
         'presaleBuildingSupportingAreaInfo',
         'presaleHouseCount',
         'presaleHousingLandIsMortgage',
         'presalePermitNumber',
         'presalePermitTie',
         'presaleRegistrationManagementDepartment',
         'presaleTotalBuidlingArea',
         'projectName',
         'realEstateProjectID',
         'recordTime',
         'remarks',
         'sourceUrl',
         'totalBuidlingArea',
         'underGroundArea',
         'validityDateClosingDate',
         'validityDateDescribe',
         'validityDateStartDate']

def recordTime():
    data = data.asDict()
    if not data['RecordTime']:
        data['RecordTime'] = str(nowtime)
    return Row(**data)

def projectName():
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)

def realEstateProjectID():
    return data

def presalePermitNumber():
    return data

def totalBuidlingArea():
    pass

def approvalPresaleAmount():
    pass

def approvalPresaleArea():
    pass

def approvalPresaleHouseAmount():
    pass

def approvalPresaleHouseArea():
    pass

def presaleBuildingAmount():
    date = data.asDict()
    pd = pd.read_sql(con=Var.ENGINE,sql="select count(distinct(BuildingID)) as col from BuildingInfoItem where PresalePermitNumber='{projectUUID}'".format(
                         projectUUID=data['PresalePermitNumber']))
    data['PresaleBuildingAmount'] = str(df.col.values[0])
    return Row(**data)

def constructionFloorCount():
    pass

def builtFloorCount():
    pass

def periodsCount():
    pass

def constructionTotalArea():
    pass

def groundArea():
    pass

def underGroundArea():
    pass

def presaleTotalBuidlingArea():
    pass

def contacts():
    data = data.asDict()
    data['contacts'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingSellPhone', ''))
    return Row(**data)

def presaleBuildingSupportingAreaInfo():
    pass

def presaleHousingLandIsMortgage():
    pass

def validityDateStartDate():
    pass

def validityDateClosingDate():
    pass

def lssueDate():
    pass

def lssuingAuthority():
    pass

def presaleRegistrationManagementDepartment():
    pass

def validityDateDescribe():
    pass

def approvalPresalePosition():
    pass

def landUse():
    pass

def earliestStartDate():
    pass

def latestDeliversHouseDate():
    pass

def earliestOpeningDate():
    pass

def houseSpread():
    pass

def presalePermitTie():
    pass

def presaleHouseCount():
    pass

def remarks():
    pass

def sourceUrl():
    pass

