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
         'validityDateStartDate',
         'projectUUID']

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

def projectUUID(data):
    return data

def presalePermitNumber(data):
    data = data.asDict()
    data['PresalePermitNumber'] = Meth.cleanName(data['PresalePermitNumber'])
    return Row(**data)

def totalBuidlingArea(data):
    return data

def approvalPresaleAmount(data):
    return data

def approvalPresaleArea(data):
    return data

def approvalPresaleHouseAmount(data):
    return data

def approvalPresaleHouseArea(data):
    return data

def presaleBuildingAmount(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,sql="select ExtraJson,BuildingID as col from BuildingInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    pd['ExtraJson'] = df['ExtraJson'].apply(Meth.jsonLoad).apply(lambda x:x.get('ExtraPresellUUID'))
    data['PresaleBuildingAmount'] = str(df.BuildingID[df.ExtraJson !=data['PresalePermitTie']].unique().size)
    return Row(**data)

def constructionFloorCount(data):
    return data

def builtFloorCount(data):
    return data

def periodsCount(data):
    return data

def constructionTotalArea(data):
    return data

def groundArea(data):
    return data

def underGroundArea(data):
    return data

def presaleTotalBuidlingArea(data):
    return data

def contacts(data):
    data = data.asDict()
    df = pd.read_sql(con=Var.ENGINE,sql="select ExtraJson as col from ProjectInfoItem where ProjectUUID='{projectUUID}'".format(
                         projectUUID=data['ProjectUUID']))
    if Meth.jsonLoad(df.col.values[0]):
        data['contacts'] = str(Meth.jsonLoad(df.col.values[0]).get('ExtraBuildingSellPhone', ''))
    return Row(**data)

def presaleBuildingSupportingAreaInfo(data):
    return data

def presaleHousingLandIsMortgage(data):
    return data

def validityDateStartDate(data):
    return data

def validityDateClosingDate(data):
    return data

def lssueDate(data):
    return data

def lssuingAuthority(data):
    return data

def presaleRegistrationManagementDepartment(data):
    return data

def validityDateDescribe(data):
    return data

def approvalPresalePosition(data):
    data = data.asDict()
    data['ApprovalPresalePosition'] = Meth.cleanName(data['ApprovalPresalePosition'])
    return Row(**data)

def landUse(data):
    return data

def earliestStartDate(data):
    return data

def latestDeliversHouseDate(data):
    return data

def earliestOpeningDate(data):
    return data

def houseSpread(data):
    return data

def presalePermitTie(data):
    return data

def presaleHouseCount(data):
    return data

def remarks(data):
    return data

def sourceUrl(data):
    return data

