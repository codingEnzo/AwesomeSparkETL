# coding=utf-8
from __future__ import unicode_literals
from sqlalchemy import create_engine
import sys
if sys.version_info.major >= 3:
    from collections import UserDict
else:
    from UserDict import UserDict

ENGINE = create_engine('mysql+pymysql://root:yunfangdata@10.30.1.7:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')

NUMTAB_TRANS = {f: t for f, t in zip('1234567890', '一二三四五六七八九零')}


class NiceDict(UserDict):

    def __init__(self, dictionary=None, target=None):
        self.data = {}
        if target is not None:
            for i, key in enumerate(target):
                self.data[key] = ""
        if dictionary is not None:
            self.update(dictionary)
        for i, (key, value) in enumerate(self.data.items()):
            if (not value) and (value != 0):
                self.data[key] = ""

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def values(self):
        return self.data.values()


PROJECT_FIELDS = ['ApprovalPresaleAmount', 'ApprovalPresaleArea', 'AveragePrice', 'BuildingPermit', 'BuildingType',
                  'CertificateOfUseOfStateOwnedLand', 'City', 'CompletionDate', 'ConstructionPermitNumber',
                  'Decoration', 'Developer', 'DistrictName', 'EarliestOpeningTime', 'EarliestStartDate', 'ExtraJson',
                  'FloorArea', 'FloorAreaRatio', 'GreeningRate', 'HouseBuildingCount', 'HouseUseType', 'HousingCount',
                  'LandLevel', 'LandUse', 'LandUsePermit', 'LatestDeliversHouseDate', 'LegalPerson',
                  'LegalPersonNumber', 'LssueDate', 'ManagementCompany', 'ManagementFees', 'OnSaleState', 'OtheRights',
                  'ParkingSpaceAmount', 'PresalePermitNumber', 'PresaleRegistrationManagementDepartment',
                  'ProjectAddress', 'ProjectApproveData', 'ProjectBookingData', 'ProjectID', 'ProjectName',
                  'ProjectType', 'ProjectUUID', 'PromotionName', 'PropertyRightsDescription', 'QualificationNumber',
                  'RealEstateProjectID', 'RecordTime', 'RegionName', 'Remarks', 'SourceUrl', 'TotalBuidlingArea']
PRESELL_FIELDS = [
    'RecordTime',
    'ProjectName',
    'RealEstateProjectID',
    'ProjectUUID',
    'PresalePermitNumber',
    'TotalBuidlingArea',
    'ApprovalPresaleAmount',
    'ApprovalPresaleArea',
    'ApprovalPresaleHouseAmount',
    'ApprovalPresaleHouseArea',
    'PresaleBuildingAmount',
    'ConstructionFloorCount',
    'BuiltFloorCount',
    'PeriodsCount',
    'ConstructionTotalArea',
    'GroundArea',
    'UnderGroundArea',
    'PresaleTotalBuidlingArea',
    'Contacts',
    'PresaleBuildingSupportingAreaInfo',
    'PresaleHousingLandIsMortgage',
    'ValidityDateStartDate',
    'ValidityDateClosingDate',
    'LssueDate',
    'LssuingAuthority',
    'PresaleRegistrationManagementDepartment',
    'ValidityDateDescribe',
    'ApprovalPresalePosition',
    'LandUse',
    'EarliestStartDate',
    'LatestDeliversHouseDate',
    'EarliestOpeningDate',
    'HouseSpread',
    'PresalePermitTie',
    'PresaleHouseCount',
    'Remarks',
    'SourceUrl',
    'ExtraJson',
    'PermitID',
    'City',
]
PERMIT_FIELDS = ['ApprovalPresaleAmount', 'ApprovalPresaleArea', 'ApprovalPresaleHouseAmount',
                 'ApprovalPresaleHouseArea', 'ApprovalPresalePosition', 'BuiltFloorCount', 'City',
                 'ConstructionFloorCount', 'ConstructionTotalArea', 'Contacts', 'EarliestOpeningDate',
                 'EarliestStartDate', 'ExtraJson', 'GroundArea', 'HouseSpread', 'LandUse', 'LatestDeliversHouseDate',
                 'LssueDate', 'LssuingAuthority', 'PeriodsCount', 'PermitID', 'PresaleBuildingAmount',
                 'PresaleBuildingSupportingAreaInfo', 'PresaleHouseCount', 'PresaleHousingLandIsMortgage',
                 'PresalePermitNumber', 'PresalePermitTie', 'PresaleRegistrationManagementDepartment',
                 'PresaleTotalBuidlingArea', 'ProjectName', 'ProjectUUID', 'RealEstateProjectID', 'RecordTime',
                 'Remarks', 'SourceUrl', 'TotalBuidlingArea', 'UnderGroundArea', 'ValidityDateClosingDate',
                 'ValidityDateDescribe', 'ValidityDateStartDate']
BUILDING_FIELDS = ['Address', 'BuildingArea', 'BuildingAveragePrice', 'BuildingCategory', 'BuildingHeight',
                   'BuildingID', 'BuildingName', 'BuildingPriceRange', 'BuildingStructure', 'BuildingType',
                   'BuildingUUID', 'ElevaltorInfo', 'ElevatorHouse', 'EstimatedCompletionDate', 'ExtraJson', 'Floors',
                   'HousingCount', 'IsHasElevator', 'OnTheGroundFloor', 'PresalePermitNumber', 'ProjectName',
                   'ProjectUUID', 'RealEstateProjectID', 'RecordTime', 'Remarks', 'SourceUrl', 'TheGroundFloor',
                   'UnitID', 'UnitName', 'UnitUUID', 'Units', 'UnsoldAmount']
HOUSE_FIELDS = ['ActualFloor', 'Address', 'Balconys', 'BuildingID', 'BuildingName', 'BuildingStructure', 'BuildingUUID',
                'CaseTime', 'City', 'Decoration', 'DecorationPrice', 'DistrictName', 'Dwelling', 'ExtraJson',
                'FloorCount', 'FloorHight', 'FloorName', 'FloorType', 'ForecastBuildingArea',
                'ForecastInsideOfBuildingArea', 'ForecastPublicArea', 'Halls', 'HouseID', 'HouseLabel',
                'HouseLabelLatest', 'HouseName', 'HouseNature', 'HouseNumber', 'HouseSalePrice', 'HouseShape',
                'HouseState', 'HouseStateLatest', 'HouseType', 'HouseUUID', 'HouseUseType', 'IsAttachment',
                'IsMortgage', 'IsMoveBack', 'IsPrivateUse', 'IsSharedPublicMatching', 'Kitchens',
                'MeasuredBuildingArea', 'MeasuredInsideOfBuildingArea', 'MeasuredSharedPublicArea',
                'MeasuredUndergroundArea', 'NatureOfPropertyRight', 'Price', 'PriceType', 'ProjectName', 'ProjectUUID',
                'RealEstateProjectID', 'RecordTime', 'Remarks', 'Rooms', 'SalePriceByBuildingArea',
                'SalePriceByInsideOfBuildingArea', 'SellSchedule', 'SellState', 'SourceUrl', 'Toilets', 'TotalPrice',
                'Toward', 'UnenclosedBalconys', 'UnitID', 'UnitName', 'UnitShape', 'UnitStructure', 'UnitUUID']
