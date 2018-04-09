# coding = utf-8


def cleanName(x):
    return x.replace('（', '(').replace('）', ')')\
        .replace('】', ']').replace('【', '[')\
        .replace('，', ',').replace('－', '-').\
        replace('〔', '[').replace('〕', ']') if x else ''


def isInt(val):
    try:
        int(val)
    except ValueError:
        return False
    else:
        return True
