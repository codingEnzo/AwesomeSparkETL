# coding=utf-8
import string

from collections import UserDict


NUMTAB = {f: t for f, t in zip('1234567890', '一二三四五六七八九零')}
FLOORTYPES = {
    -1: '低层(1-3)',
    0: '',
    1: '低层(1-3)',
    3: '低层(1-3)',
    6: '多层(4-6)',
    11: '小高层(7-11)',
    18: '中高层(12-18)',
    32: '高层(19-32)',
    33: '超高层(33)',
}
PROJECT_FIELDS = [
    'RecordTime',
    'ProjectName',
    'PromotionName',
    'RealEstateProjectID',
    'ProjectUUID',
    'DistrictName',
    'RegionName',
    'ProjectAddress',
    'ProjectType',
    'OnSaleState',
    'LandUse',
    'HousingCount',
    'Developer',
    'FloorArea',
    'TotalBuidlingArea',
    'BuildingType',
    'HouseUseType',
    'PropertyRightsDescription',
    'ProjectApproveData',
    'ProjectBookingData',
    'LssueDate',
    'PresalePermitNumber',
    'HouseBuildingCount',
    'ApprovalPresaleAmount',
    'ApprovalPresaleArea',
    'AveragePrice',
    'EarliestStartDate',
    'CompletionDate',
    'EarliestOpeningTime',
    'LatestDeliversHouseDate',
    'PresaleRegistrationManagementDepartment',
    'LandLevel',
    'GreeningRate',
    'FloorAreaRatio',
    'ManagementFees',
    'ManagementCompany',
    'OtheRights',
    'CertificateOfUseOfStateOwnedLand',
    'ConstructionPermitNumber',
    'QualificationNumber',
    'LandUsePermit',
    'BuildingPermit',
    'LegalPersonNumber',
    'LegalPerson',
    'SourceUrl',
    'Decoration',
    'ParkingSpaceAmount',
    'Remarks',
    'ExtraJson',
    'ProjectID',
    'City',
]
BUILDING_FIELDS = [
    'RecordTime',
    'ProjectName',
    'RealEstateProjectID',
    'BuildingName',
    'BuildingID',
    'BuildingUUID',
    'UnitName',
    'UnitID',
    'PresalePermitNumber',
    'Address',
    'OnTheGroundFloor',
    'TheGroundFloor',
    'EstimatedCompletionDate',
    'HousingCount',
    'Floors',
    'ElevatorHouse',
    'IsHasElevator',
    'ElevaltorInfo',
    'BuildingStructure',
    'BuildingType',
    'BuildingHeight',
    'BuildingCategory',
    'Units',
    'UnsoldAmount',
    'BuildingAveragePrice',
    'BuildingPriceRange',
    'BuildingArea',
    'Remarks',
    'SourceUrl',
    'ExtraJson',
    'ProjectUUID',
    'UnitUUID',

]
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
HOUSE_FIELDS = [
    'RecordTime',
    'CaseTime',
    'ProjectName',
    'RealEstateProjectID',
    'BuildingName',
    'BuildingID',
    'City',
    'DistrictName',
    'UnitName',
    'UnitID',
    'HouseNumber',
    'HouseName',
    'HouseID',
    'Address',
    'FloorName',
    'ActualFloor',
    'FloorCount',
    'FloorType',
    'FloorHight',
    'UnitShape',
    'UnitStructure',
    'Rooms',
    'Halls',
    'Kitchens',
    'Toilets',
    'Balconys',
    'UnenclosedBalconys',
    'HouseShape',
    'Dwelling',
    'ForecastBuildingArea',
    'ForecastInsideOfBuildingArea',
    'ForecastPublicArea',
    'MeasuredBuildingArea',
    'MeasuredInsideOfBuildingArea',
    'MeasuredSharedPublicArea',
    'MeasuredUndergroundArea',
    'Toward',
    'HouseType',
    'HouseNature',
    'Decoration',
    'NatureOfPropertyRight',
    'HouseUseType',
    'BuildingStructure',
    'HouseSalePrice',
    'SalePriceByBuildingArea',
    'SalePriceByInsideOfBuildingArea',
    'IsMortgage',
    'IsAttachment',
    'IsPrivateUse',
    'IsMoveBack',
    'IsSharedPublicMatching',
    'SellState',
    'SellSchedule',
    'HouseState',
    'HouseStateLatest',
    'HouseLabel',
    'HouseLabelLatest',
    'TotalPrice',
    'Price',
    'PriceType',
    'DecorationPrice',
    'Remarks',
    'SourceUrl',
    'ExtraJson',
    'ProjectUUID',
    'BuildingUUID',
    'HouseUUID',
    'UnitUUID',
]
CASE_FIELDS = [
    'ProjectID',
    'BuildingID',
    'HouseID',
    'ForecastBuildingArea',
    'ForecastInsideOfBuildingArea',
    'ForecastPublicArea',
    'MeasuredBuildingArea',
    'MeasuredInsideOfBuildingArea',
    'MeasuredSharedPublicArea',
    'IsMortgage',
    'IsAttachment',
    'IsPrivateUse',
    'IsMoveBack',
    'IsSharedPublicMatching',
    'BuildingStructure',
    'SellSchedule',
    'SellState',
    'SourceUrl',
    'CaseTime',
    'CaseFrom',
    'UnitShape',
    'UnitStructure',
    'Balconys',
    'UnenclosedBalconys',
    'DistrictName',
    'RegionName',
    'ProjectName',
    'BuildingName',
    'PresalePermitNumber',
    'HouseName',
    'HouseNumber',
    'TotalPrice',
    'Price',
    'PriceType',
    'Address',
    'BuildingCompletedYear',
    'ActualFloor',
    'NominalFloor',
    'Floors',
    'HouseUseType',
    'Dwelling',
    'State',
    'DealType',
    'Remarks',
    'RecordTime',
    'ProjectUUID',
    'BuildingUUID',
    'HouseUUID',
    'UnitUUID',
]


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
