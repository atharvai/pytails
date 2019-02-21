from bson import Timestamp


def bson_timestamp_to_int(ts: Timestamp):
    # https://docs.mongodb.com/manual/reference/bson-types/#timestamps
    return (ts.time << 32) + ts.inc


def int_to_bson_timestamp(ts: int):
    # https://docs.mongodb.com/manual/reference/bson-types/#timestamps
    t = ts >> 32
    i = ts & (2 ** 32 - 1)
    return Timestamp(time=t, inc=i)
