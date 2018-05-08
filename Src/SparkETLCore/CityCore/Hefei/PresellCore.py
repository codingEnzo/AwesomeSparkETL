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

def recordTime(data):
    data = data.asDict()
    if not data['RecordTime']:
        data['RecordTime'] = str(nowtime)
    return Row(**data)

def projectName(data):
    data = data.asDict()
    data['ProjectName'] = Meth.cleanName(data['ProjectName'])
    return Row(**data)

def realEstateProjectID(data):
    return data

def presalePermitNumber(data):
    return data

def totalBuidlingArea(data):
    pass

def approvalPresaleAmount(data):
    pass

def approvalPresaleArea(data):
    pass

def approvalPresaleHouseAmount(data):
    pass

def approvalPresaleHouseArea(data):
    pass

def presaleBuildingAmount(data):
    date = data.asDict()
    pd = pd.read_sql(con=Var.ENGINE,sql="select count(distinct(BuildingID)) as col from BuildingInfoItem where PresalePermitNumber='{projectUUID}'".format(
                         projectUUID=data['PresalePermitNumber']))
    data['PresaleBuildingAmount'] = str(df.col.values[0])
    return Row(**data)

def constructionFloorCount(data):
    pass

def builtFloorCount(data):
    pass

def periodsCount(data):
    pass

def constructionTotalArea(data):
    pass

def groundArea(data):
    pass

def underGroundArea(data):
    pass

def presaleTotalBuidlingArea(data):
    pass

def contacts(data):
    data = data.asDict()
    data['contacts'] = str(Meth.jsonLoad(data['ExtraJson']).get('ExtraBuildingSellPhone', ''))
    return Row(**data)

def presaleBuildingSupportingAreaInfo(data):
    pass

def presaleHousingLandIsMortgage(data):
    pass

def validityDateStartDate(data):
    pass

def validityDateClosingDate(data):
    pass

def lssueDate(data):
    pass

def lssuingAuthority(data):
    pass

def presaleRegistrationManagementDepartment(data):
    pass

def validityDateDescribe(data):
    pass

def approvalPresalePosition(data):
    pass

def landUse(data):
    pass

def earliestStartDate(data):
    pass

def latestDeliversHouseDate(data):
    pass

def earliestOpeningDate(data):
    pass

def houseSpread(data):
    pass

def presalePermitTie(data):
    pass

def presaleHouseCount(data):
    pass

def remarks(data):
    pass

def sourceUrl(data):
    pass

