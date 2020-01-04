#!/usr/bin/env python

import argparse
import os

import logging
import sys

from pytails.mongo.oplog_client import OplogClient
from pytails.sinks import ConsoleSink
from pytails.sinks.kinesis import KinesisSink
from pytails.sinks.firehose import FirehoseSink
from pytails.state.ddb_store import DynamoDbStore

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(Func)s %(Op)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo-host', type=str, default=os.environ.get('MONGO_HOST', None),
                        help='MongoDB replica set host to tail', required=True)
    parser.add_argument('--mongo-port', type=int, default=os.environ.get('MONGO_PORT', 27017),
                        help='MongoDB replica set port', required=True)
    parser.add_argument('--tail-id', type=str, default=os.environ.get('TAIL_ID', None),
                        help='Unique identifier for tail. usually short name for Mongodb cluster', required=True)
    parser.add_argument('--replica-set', type=str, default=os.environ.get('REPLICA_SET', None),
                        help='Mongodb Replica set name', required=True)
    parser.add_argument('--kinesis-data-sink', type=str, default=os.environ.get('KINESIS_DATA_SINK', None),
                        help='Kinesis Data Stream Name. Not ARN')
    parser.add_argument('--firehose-data-sink', type=str, default=os.environ.get('FIREHOSE_DATA_SINK', None),
                        help='Kinesis Data Stream Name. Not ARN')
    parser.add_argument('--console-sink', action='store_true', default=bool(os.environ.get('CONSOLE_SINK', 0)),
                        help='Enable Console output')
    parser.add_argument('--mode', choices=['oplog', 'full', 'cdc'], default=os.environ.get('MODE', 'oplog'),
                        help='oplog: oplog entries \n'
                             'full: full document\n'
                             'cdc: Enable for MongoDB v3.6 Change Streams')
    parser.add_argument('--debug', action='store_true', default=bool(os.environ.get('DEBUG', 0)),
                        help='Enable for MongoDB v3.6 Change Streams')
    parser.add_argument('--set-timestamp', action='store_true', help='Adds timestamp to entry')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger('').setLevel(logging.DEBUG)
    if args.mode == 'cdc':
        raise NotImplementedError('CDC Mode not implemented')
    else:
        if args.replica_set:
            client = OplogClient(args.mongo_host, args.mongo_port, args.tail_id, args.replica_set)
        else:
            client = OplogClient(args.mongo_host, args.mongo_port, args.tail_id)

        if args.set_timestamp:
            client.set_timestamp_suffix()

        if args.mode == 'full':
            client.set_full_doc()

        if args.console_sink:
            client.register_data_sink(ConsoleSink(client.identifier))
        if args.kinesis_data_sink:
            client.register_data_sink(KinesisSink(client.identifier, args.kinesis_data_sink))
        if args.firehose_data_sink:
            client.register_data_sink(FirehoseSink(client.identifier, args.firehose_data_sink))
        client.tail()


main()
