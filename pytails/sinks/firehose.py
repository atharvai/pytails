import time

import boto3
from botocore.exceptions import ClientError
from bson import json_util

from .sink import Sink


class FirehoseSink(Sink):
    """
    Enables writing documents to an AWS Kinesis Firehose Delivery Stream.
    """
    firehose_stream_name = None
    __firehose_client = None

    _buffer = []

    def __init__(self, identifier: str, firehose_stream_name: str):
        super().__init__(identifier)
        self.__firehose_client = boto3.client('firehose', verify=False)
        self.firehose_stream_name = firehose_stream_name

    def write_record(self, obj: dict) -> None:
        """
        Writes document to a Firehose Data Stream as a single record.

        :param obj: dict.
        :return:
        """
        obj_str = f'{json_util.dumps(obj)}\n'
        self._buffer.append({'Data': obj_str})
        if len(self._buffer) == 500:
            try:
                self.__firehose_client.put_record_batch(DeliveryStreamName=self.firehose_stream_name,
                                                        Records=self._buffer)
                self._buffer = []
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'ServiceUnavailableException':
                    time.sleep(1)
                    self.write_record(obj)
