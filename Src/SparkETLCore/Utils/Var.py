# coding=utf-8
import os 
from sqlalchemy import *
# sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
# ENGINE = create_engine(
#     'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')
# MIRROR_ENGINE = create_engine(
#     'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_mirror?charset=utf8')
ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.7:3307/spark_test?charset=utf8')
MIRROR_ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.7:3307/spark_mirror?charset=utf8')
NUMTAB = {ord(f): ord(t) for f, t in zip('1234567890', '一二三四五六七八九')}
SYMBOLSTAB = {ord(f):ord(t) for f,t in zip('，。！？【】（）％＃＠＆１２３４５６７８９０',',.!?[]()%#@&1234567890')}

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
DEAL_FIELDS = [
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
SUPPLY_FIELDS = [
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
    'Remarks',
    'RecordTime',
    'ProjectUUID',
    'BuildingUUID',
    'HouseUUID',
    'UnitUUID',]

QUIT_FIELDS = [
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
    'Remarks',
    'RecordTime',
    'ProjectUUID',
    'BuildingUUID',
    'HouseUUID',
    'UnitUUID',]