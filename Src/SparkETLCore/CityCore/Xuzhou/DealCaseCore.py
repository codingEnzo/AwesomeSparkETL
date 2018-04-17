from pyspark.sql import Row


def realEstateProjectId(data):
    return data


def buildingId(data):
    return data


def houseId(data):
    return data


def forecastBuildingArea(data):
    return data


def forecastInsideOfBuildingArea(data):
    return data


def forecastPublicArea(data):
    return data


def measuredBuildingArea(data):
    return data


def measuredInsideOfBuildingArea(data):
    return data


def measuredSharedPublicArea(data):
    return data


def isMortgage(data):
    return data


def isAttachment(data):
    return data


def isPrivateUse(data):
    return data


def isMoveBack(data):
    return data


def isSharedPublicMatching(data):
    return data


def buildingStructure(data):
    data = data.asDict()
    data['BuildingStructure'] = data['BuildingStructure'].replace('钢混', '钢混结构') \
                                                         .replace('框架', '框架结构') \
                                                         .replace('钢筋混凝土', '钢混结构') \
                                                         .replace('混合', '混合结构') \
                                                         .replace('砖混', '砖混结构') \
                                                         .replace('框剪', '框架剪力墙结构') \
                                                         .replace('结构结构', '结构') \
                                                         .replace('钢、', '')
    data = Row(**data)
    return data


def sellSchedule(data):
    return data


def sellState(data):
    return data


def sourceLink(data):
    return data


def caseTime(data):
    return data


def caseFrom(data):
    return data


def unitShape(data):
    return data


def unitStructure(data):
    return data


def balconys(data):
    return data


def unEnclosedBalconys(data):
    return data


def districtName(data):
    data = data.asDict()
    data['DistrictName'] = data['DistrictName'].replace('金山桥经济开发区', '经济技术开发区')
    data = Row(**data)
    return data


def regionName(data):
    return data


def projectName(data):
    return data


def buildingName(data):
    return data


def presalePermitNumber(data):
    data = data.asDict()
    data['PresalePermitNumber'] = data['PresalePermitNumber'].replace(
        '（', '(').replace('）', ')')
    return data


def houseName(data):
    data = data.asDict()
    data['HouseName'] = data['UnitName'] + '单元' \
        + data['Floor'] + '层' + str(data['HouseNumber'])
    data = Row(**data)
    return data


def houseNumber(data):
    return data


def totalPrice(data):
    return data


# TODO: Waiting.
def price(data):
    data = data.asDict()
    pid = data['ProjectUUID']
    sql = """
    SELECT ProjectInfoItem.AveragePrice FROM ProjectInfoItem
    WHERE ProjectInfoItem.ProjectUUID != {}
    """.format(pid)
    return data


def priceType(data):
    data = data.asDict()
    data['PriceType'] = '项目均价'
    data = Row(**data)
    return data


def address(data):
    return data


def buildingCompletedYear(data):
    return data


def floor(data):
    return data


def nominalFloor(data):
    return data


def floors(data):
    return data


def houseUseType(data):
    return data


def dwelling(data):
    return data


def state(data):
    data = data.asDict()
    if data['HouseStateLatest'] == '可销售':
        data['State'] = '明确成交'
    else:
        data['State'] = '历史成交'
    data = Row(**data)
    return data


def dealType(data):
    return data


def remarks(data):
    return data
