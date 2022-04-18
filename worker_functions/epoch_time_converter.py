import datetime


def get_epoch_time(tt):
    y, mo, d, h, m, s = tt
    ts = (datetime.datetime(y, mo, d, h, m, s) - datetime.datetime(1970, 1, 1)).total_seconds()
    return int(ts)
