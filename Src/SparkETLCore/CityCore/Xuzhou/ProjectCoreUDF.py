# -*- coding: utf-8 -*-
import demjson
import sys

from SparkETLCore.Utils.Meth import cleanName

METHODS = {
    'PresellInfoItem': [
        'constructionPermitNumber',
        'floorArea',
        'landUse',
        'landUsePermit',
        'lssueDate',
        'presalePermitNumber',
        'recordTime',
        'regionName',
    ],
    'HouseInfoItem': [
        'houseUseType',
    ]
}


def recordTime(pdf):
    record_time = pdf.RecordTime
    import datetime
    nt = datetime.datetime.now()
    record_time = record_time.apply(lambda t: nt if not t else t)
    return pdf.assign(RecordTime=record_time)


def regionName(pdf):
    extra_json = pdf.ExtraJson
    extra_region_name = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraRegionName', '')).unique()
    return pdf.assign(ResionName=demjson.encode(extra_region_name))


def landUse(pdf):
    land_use = pdf.LandUse[pdf.LandUse != '']
    land_use = land_use.apply(
        lambda x: x.replace('宅', '住宅') \
        .replace('宅宅', '宅') \
        .replace('住住', '住') \
        .replace('、', '/') \
        .replace('，', ',').strip('/,')
    )
    _tmp = list(set(','.join(land_use).split(',')))
    return pdf.assign(LandUse=demjson.encode(_tmp))


def floorArea(pdf):
    extra_json = pdf.ExtraJson
    extra_floor_area = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraFloorArea', ''))
    extra_land_cert = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraLandCertificate', ''))
    pdf.assign(ExtraFloorArea=extra_floor_area)
    pdf.assign(ExtraLandCertificate=extra_land_cert)

    # >>> 待定


def houseUseType(pdf):
    house_use_type = pdf.HouseUseType
    return pdf.assign(HouseUseType=demjson.encode(house_use_type.unique()))


def lssueDate(pdf):
    lssue_date = pdf.LssueDate
    lssue_date = lssue_date.apply(lambda x: cleanName(x))
    lssue_date = lssue_date[lssue_date != '']
    return pdf.assign(LssueDate=demjson.encode(lssue_date.unique()))


def presalePermitNumber(pdf):
    pre_permit_num = pdf.PresalePermitNumber
    pre_permit_num = pre_permit_num.apply(lambda x: cleanName(x))
    pre_permit_num = pre_permit_num[pre_permit_num != '']
    return pdf.assign(
        PresalePermitNumber=demjson.encode(pre_permit_num.unique()))


def certificateOfUseOfStateOwnedLand(pdf):
    extra_json = pdf.ExtraJson
    extra_cert_state = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraCertificateOfUseOfStateOwnedLand', '')
    )
    return pdf.assign(
        CertificateOfUseOfStateOwnedLand=demjson.encode(
            extra_cert_state.unique()))


def constructionPermitNumber(pdf):
    extra_json = pdf.ExtraJson
    extra_const_permit = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraConstructionPermitNumber', ''))
    return pdf.assign(
        ConstructionPermitNumber=demjson.encode(extra_const_permit.unique()))


def landUsePermit(pdf):
    extra_json = pdf.ExtraJson
    extra_land_cert = extra_json.apply(
        lambda j: demjson.decode(j).get('ExtraLandCertificate', '').replace('、', ''))
    return pdf.assign(
        LandUsePermit=demjson.encode(extra_land_cert[extra_land_cert != '']
                                     .unique()))
