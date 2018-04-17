import re
import demjson

from Utils.Meth import isInt


def caseTime(data):
    return data


def projectName(data):
    return data


def realEstateProjectId(data):
    return data


def buildingName(data):
    return data


def buildingId(data):
    return data


def city(data):
    return data


def districtname(data):
    df = data['ProjectDF']
    dSeq = df['DistrictName'][df.ProjectUUID == data['ProjectUUID']]
    dName = '' if dSeq.empty else dSeq.first(0)
    return dName.replace('金山桥经济开发区', '经济技术开发区')


def unitName(data):
    return data


def unitId(data):
    return data


def houseNumber(data):
    return data


def houseName(data):
    hName = data['unitName'] + '单元' \
        + data['floorName'] + '层' \
        + str(data['houseNumber'])
    return hName


def houseId(data):
    return data


def houseUUID(data):
    return data


def address(data):
    df = data['ProjectDF']
    aSeq = df['ProjectAddress'][df.ProjectUUID == data['ProjectUUID']]
    addr = '' if aSeq.empty else aSeq.first(0)
    return addr


def floorName(data):
    hName = data.lower().replace('铺', '').replace('阁', ''). \
        replace('库', '').replace('跃', ''). \
        replace('j', '').replace('车', ''). \
        replace('商', '').replace('楼', ''). \
        replace('gl', '').replace('g1', '')
    floor = '1'
    if isInt(hName):
        _ = int(hName)
        if (len(hName) - 4) > 0:
            offset = len(hName) - 4 if _ > 0 else len(hName) - 3
            return '{}'.format(hName[:offset])
        elif (len(hName) - 2) > 0:
            offset = len(hName) - 2 if _ > 0 else len(hName) - 1
            return '{}'.format(hName[:offset])
    else:
        if re.search(r'^[a-z]', hName):
            if re.search(r'\d+', hName):
                _floor = re.search(r'\d+', hName).group(0)
                if (len(_floor) - 4) > 0:
                    offset = len(_floor) - 4
                    return '{}'.format(_floor[:offset])
                elif (len(_floor) - 2) > 0:
                    offset = len(_floor) - 2
                    return '{}'.format(_floor[:offset])
        elif '-' in hName:
            if hName.startswith('-35-'):
                _ = re.search(r'\-.+\-(.+)', hName)
                if _:
                    _floor = _.group(1)
                    if len(_floor) - 2 > 0:
                        return _floor[1]
                    else:
                        return '-1'
            elif hName.startswith('-'):
                if re.search(
                        r'^-[a-z]',
                        hName) or len(hName) < 4 or hName.startswith('-0'):
                    return '-1'
                elif re.search(r'^-[1-9]', hName):
                    return hName[:1]
            else:
                _floor = re.search(r'-(.+)', hName).group(1)
                _floor = re.sub(r'[\u4E00-\u9FA5]', '', _floor)
                if _floor.count('-') == 2:
                    _floor = re.search(r'-(\d+)-', _floor).group(1)
                elif (not _floor.startswith('0')) and (not re.search(
                        r'^[a-z]', _floor)):
                    if len(_floor) - 4 > 0:
                        offset = len(_floor) - 4
                        return _floor[:offset]
                    elif len(_floor) - 2 > 0:
                        offset = len(_floor) - 4
                        return _floor[:offset]

        elif re.search(r'[\u4E00-\u9FA5]', hName):
            if '号' in hName or '单元' in hName:
                _ = re.search(r'号(\d+)|单元(\d+)', hName)
                if _:
                    _floor = _.group(1)
                    if (len(_floor) - 5 < 0) and (len(_floor) - 2 > 0):
                        gap = len(_floor) - 2
                        floor = _floor[:gap]
    return floor


def actualFloor(data):
    return data


def floorCount(data):
    return data


def floorType(data):
    return data


def floorRight(data):
    return data


def unitShape(data):
    return data


def unitStructure(data):
    return data


def rooms(data):
    return data


def halls(data):
    return data


def kitchens(data):
    return data


def toilets(data):
    return data


def balconys(data):
    return data


def unEnclosedBalconys(data):
    return data


def houseShape(data):
    return data


def dwelling(data):
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


def measuredUndergroundArea(data):
    return data


def toward(data):
    return data


def houseType(data):
    return data


def houseNature(data):
    return data


def decoration(data):
    return data


def natureOfPropertyRight(data):
    return data


def houseUseType(data):
    return data


def buildingStructure(data):
    data = data.replace('钢混', '钢混结构') \
        .replace('框架', '框架结构') \
        .replace('钢筋混凝土', '钢混结构') \
        .replace('混合', '混合结构') \
        .replace('结构结构', '结构') \
        .replace('砖混', '砖混结构') \
        .replace('框剪', '框架剪力墙结构') \
        .replace('钢、', '')
    return data


def houseSalePrice(data):
    return data


def salePriceByBuildingArea(data):
    return data


def salePriceByInsideOfBuildingArea(data):
    return data


def isMortgage(data):
    return data


def isAttachment(data):
    return data


def isPrivateUse(data):
    return data


def isMoveback(data):
    return data


def isSharedPublicMatching(data):
    return data


def sellState(data):
    return data


def sellSchedule(data):
    return data


def houseState(data):
    return data


def houseStateLatest(data):
    return data


def houseLabel(data):
    return data


def houseLabelLatest(data):
    return data


def totalPrice(data):
    return data


def price(data):
    return data


def priceType(data):
    return data


def decorationPrice(data):
    return data


def remarks(data):
    return data


def sourceUrl(data):
    return data


def extraJSON(data):
    _ = demjson.encode(data)
    return _
