# coding = utf-8
import datetime
import demjson

from pyspark.sql import Row
ENGINE = getEngine("spark_test")


def recordtime(data):
    if not data.get("RecordTime"):
        nt = datetime.datetime.now()
        _d = data.asDict()
        _d['RecordTime'] = nt
        data = Row(**_d)
    return data


def projectName(data):
    return data


def promotionName(data):
    return data


def realestateProjectId(data):
    return data


def projectUUID(data):
    return data


def districtName(data):
    return data


def regionName(data):
    return data


def projectAddress(data):
    return data


def projectType(data):
    return data


def onSaleState(data):
    return data


def landUse(data):
    return data


def housingCount(data):
    return data


def developer(data):
    return data


def floorArea(data):
    return data


def totalBuidlingArea(data):
    return data


def buildingType(data):
    return data


def houseUseType(data):
    return data


def propertyRightsDescription(data):
    return data


def projectApproveData(data):
    return data


def projectBookingData(data):
    return data


def lssueDate(data):
    return data


def presalePermitNumber(data):
    return data


def houseBuildingCount(data):
    return data


def approvalPresaleAmount(data):
    return data


def approvalPresaleArea(data):
    return data


def averagePrice(data):
    return data


def earliestStartDate(data):
    return data


def completionDate(data):
    return data


def earliestOpeningTime(data):
    return data


def latestDeliversHouseDate(data):
    return data


def presaleRegistrationManagementDepartment(data):
    return data


def landLevel(data):
    return data


def greeningRate(data):
    return data


def floorAreaRatio(data):
    return data


def managementFees(data):
    return data


def managementCompany(data):
    return data


def otheRights(data):
    return data


def certificateOfUseOfStateOwnedLand(data):
    return data


def constructionPermitNumber(data):
    return data


def qualificationNumber(data):
    return data


def landUsePermit(data):
    return data


def buildingPermit(data):
    return data


def legalPersonNumber(data):
    return data


def legalPerson(data):
    return data


def sourceUrl(data):
    return data


def decoration(data):
    return data


def parkingSpaceAmount(data):
    return data


def remarks(data):
    return data


def extraJSON(data):
    data = data.asDict()
    extraj_origin = data.get('ExtraJson')
    if extraj_origin:
        extraj_origin = demjson.decode(extraj_origin)
        extraj = {
            'TotalBuidlingArea': extraj_origin['TotalBuidlingArea'],
            'ExtraSaleAddress': extraj_origin['ExtraSaleAddress'],
            'ExtraProjectPoint': extraj_origin['ExtraProjectPoint'],
            'ExtraSoldAmount': extraj_origin['ExtraSoldAmount'],
            'ExtraSoldArea': extraj_origin['ExtraSoldArea'],
            'ExtraUnsoldAmount': extraj_origin['ExtraUnsoldAmount'],
            'ExtraUnsoldArea': extraj_origin['ExtraUnsoldArea'],
        }
        data['ExtraJson'] = extraj
    return data
