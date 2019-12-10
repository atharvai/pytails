import time

import pymongo
from bson import Timestamp
from pymongo import MongoClient, CursorType
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError

import logging
from ..helpers.bson_utils import bson_timestamp_to_int, int_to_bson_timestamp
from .tail_client import TailClient

logger = logging.getLogger(__name__)


class OplogClient(TailClient):
    __cursor = None
    __continue_running = True
    _doc_counter = 0

    options = dict(timestamp_suffix=False,
                   full_doc=False)

    def __init__(self, mongo_host: str, mongo_port: int, cluster: str, replica_set: str = None,
                 start_ts: Timestamp = None):
        """
        Initialises a MongoClient. If `replica_set` is specified, it creates a HA connection to the set. If `start_ts`
        is set it starts tailing at the specified timestamp. if `start_ts` is not set, the client starts tailing from
        earliest available oplog entry.

        :param mongo_host: Hostname or IP Address
        :param mongo_port: port number
        :param cluster: Friendly name for cluster
        :param replica_set: Replica set name. Optional.
        :param start_ts: Oplog Timestamp. Optional.
        """
        super().__init__(cluster, replica_set)
        if replica_set:
            self._client = MongoClient(host=[f'{mongo_host}:{mongo_port}'], replicaset=replica_set)
        else:
            self._client = MongoClient(host=mongo_host, port=mongo_port)

        self._connection_check()

        if start_ts:
            self.ts = start_ts
        else:
            chkpoint_ts = self._checkpoint_store.read_state_by_key()
            if chkpoint_ts:
                self.ts = chkpoint_ts

    def _connection_check(self, attempt: int = 3):
        """
        Performs a simple check of `is_primary` to initiate a connection to the MongoDB host.
        Connection attempt is tried 3 times before raising Error.

        :param attempt: int. default: 3
        :return: bool.
        """
        attempt -= 1
        try:
            return self._check_mongo_version
        except ServerSelectionTimeoutError as ex:
            if attempt == 0:
                raise ex
            time.sleep(5)
            self._connection_check(attempt)

    def set_timestamp_suffix(self, value: bool = True) -> None:
        """
        Adds timestamp to the output document

        :param value: bool. default: True
        """
        self.options['timestamp_suffix'] = value

    def set_full_doc(self, value: bool = True) -> None:
        # checkpointing breaks if this is enabled. figure out how to make this work.
        self.options['full_doc'] = value
        self.set_timestamp_suffix()

    @property
    def _check_mongo_version(self) -> str:
        """
        Returns the version of MongoDB server

        :rtype: str
        :return:
        """
        return self._client.server_info()['version']

    def get_cursor(self, ts: Timestamp = None):
        """
        Returns a tailable cursor for oplog.rs collection

        :param ts: Timestamp. Default: None.
        :return:
        """
        if not ts:
            ts = self.ts
        oplog = self._client.local.oplog.rs
        self.__cursor = oplog.find({'ts': {'$gte': ts}},
                                   cursor_type=CursorType.TAILABLE_AWAIT,
                                   oplog_replay=True)
        return self.__cursor

    def get_oplog_first_ts(self) -> Timestamp:
        """
        Returns the earliest available timestmap for oplog

        :return: Timestamp
        """
        oplog = self._client.local.oplog.rs
        first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
        return first['ts']

    def tail(self) -> None:
        """
        Tails the oplog.rs entries and processes them.

        At least one data sink must be registered. if not, NotImplementedError is raised.

        For every oplog document, `process_doc` method is called. If connection is severed or primary changes, the client
        attempts to connect to new primary server.
        :return:
        """
        if not self._data_sinks:
            raise NotImplementedError('data sink not registered')
        logger.info(extra=dict(Func='Start', Op='Tail',
                               Attributes={'identifier': self.identifier, 'host': self._client.address[0],
                                           'port': self._client.address[1]}), msg='')
        self.__continue_running = True
        while self.__continue_running:
            self.get_cursor(ts=self.ts)
            while self.__cursor.alive:
                try:
                    for doc in self.__cursor:
                        self.process_doc(doc)
                        if not self.__continue_running:
                            break
                except AutoReconnect as ex:
                    time.sleep(1)
                if not self.__continue_running:
                    break
                time.sleep(1)

    def process_doc(self, doc):
        """
        Processes the oplog doc to filter out noop and cmd operations and write the rest to all data sinks.

        if `full_doc` is set to True in options, a query is made to MongoDB to read the full document for every oplog
        document. For high rate of change, this might be expensive operation for the MongoDB cluster.
        Currently, `full_doc` option is not compatible with writing to data sinks.

        :param doc:
        :return:
        """
        # skip n,c ops
        if doc['op'] in ('n', 'c'):
            return

        doc_doc_ = {'doc': doc}
        if self.options['full_doc']:
            # query the full document and return it
            oid = None
            if doc['op'] in ['i', 'd']:
                oid = doc['o']['_id']
            elif doc['op'] == 'u':
                oid = doc['o2']['_id']

            if oid:
                full_doc = self._client.get_database(doc['ns'].split('.')[0]).get_collection(
                    doc['ns'].split('.')[1]).find_one({'_id': oid})
                if self.options['timestamp_suffix']:
                    full_doc = {'ts': bson_timestamp_to_int(doc['ts']), 'doc': full_doc}
                else:
                    full_doc = {'doc': full_doc}
                self.write_to_sink(full_doc)
        else:
            # return oplog without modifications
            self.write_to_sink(doc_doc_)
        self._doc_counter += 1
        if self._doc_counter >= 500:
            self.checkpoint(doc_doc_)
            self._doc_counter = 0

    def stop_tail(self):
        """
        Stops tailing the oplog collection gracefully. Closes the cursor and commits a checkpoint.

        :return:
        """
        self.__cursor.close()
        super().stop_tail()

        self.__continue_running = False

    def checkpoint(self, doc: dict = None):
        """
        Commits a checkpoint. `doc` must contain `['doc']['ts']` properties.

        Currently breaks if `full_doc` option is set.

        :param doc:
        :return:
        """
        if doc:
            if self.options['full_doc'] and self.options['timestamp_suffix']:
                self.ts = int_to_bson_timestamp(doc['ts'])
            else:
                self.ts = doc['doc']['ts']
        super().checkpoint()
