# coding=utf-8
from sqlalchemy import create_engine
from UserDict import UserDict

ENGINE = create_engine(
    'mysql+pymysql://root:gh001@10.30.1.70:3307/spark_test?charset=utf8')

NUMTAB_TRANS = str.maketrans('1234567890', '一二三四五六七八九')
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
    'HouseType'
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
BUILDING_FIELDS = []
HOUSE_FIELDS = []
DEAL_FIELDS = []


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
