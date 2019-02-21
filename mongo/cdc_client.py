from datetime import datetime

from bson import Timestamp
from pymongo import MongoClient


class ChangeStreamClient:
    """
    Not functional yet.
    """
    __client = None
    ts = Timestamp(datetime.utcnow(), 1)

    def __init__(self, mongo_host: str, mongo_port: int = 27017, start_ts: Timestamp = None):
        self.__client = MongoClient(host=mongo_host, port=mongo_port)
        if start_ts:
            self.ts = start_ts
