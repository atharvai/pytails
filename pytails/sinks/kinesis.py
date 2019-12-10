import time
import uuid

import boto3
from botocore.exceptions import ClientError
from bson import json_util

from .sink import Sink


class KinesisSink(Sink):
    """
    Enables writing documents to an AWS Kinesis Data Stream
    """
    kinesis_stream_name = None
    __kinesis_client = None

    def __init__(self, identifier: str, kinesis_stream_name: str):
        super().__init__(identifier)
        self.__kinesis_client = boto3.client('kinesis')
        self.kinesis_stream_name = kinesis_stream_name

    def write_record(self, obj: dict) -> None:
        """
        Writes document to Kinesis Data Stream.

        :param obj:
        :return:
        """
        obj_str = json_util.dumps(obj)
        try:
            try:
                part_key = obj['o']['_id']['$oid']
            except:
                part_key = str(uuid.uuid4())
            self.__kinesis_client.put_record(StreamName=self.kinesis_stream_name,
                                             Data=obj_str,
                                             PartitionKey=part_key)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                time.sleep(1)
