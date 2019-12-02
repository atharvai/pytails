from datetime import datetime

import boto3
from botocore.exceptions import ClientError

import logging
from ..helpers.bson_utils import int_to_bson_timestamp
from .store import StateStore

logger = logging.getLogger(__name__)


class DynamoDbStore(StateStore):
    """
    Enables AWS DynamoDB as a checkpoint state store.
    """
    __store = None
    state_key_cluster = None
    state_key_replicaset = None
    _state_partition_key_cluster = 'cluster'
    _state_partition_key_rs = 'replicaset'
    _store_name = 'pytails_checkpoints'

    def __init__(self, cluster: str, replica_set: str):
        self.setup_store()
        self.state_key_cluster = cluster
        self.state_key_replicaset = replica_set

    def setup_store(self):
        """
        Creates DynamoDB table.

        :return:
        """
        ddb = boto3.resource('dynamodb')
        ddb_client = boto3.client('dynamodb', verify=False)
        create_table = False
        try:
            ddb_client.describe_table(TableName=self._store_name)
            self.__store = ddb.Table(self._store_name)
            logger.info(f'DynamoDB state table discovered: {self._store_name}', extra=dict(Func='Discover', Op='StateStore'))
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'ResourceNotFoundException':
                create_table = True
                logger.warning(ex, extra=dict(Func='', Op=''))
            else:
                logger.error(ex, extra=dict(Func='', Op=''))
                raise ex
        if create_table:
            logger.info(f'Creating DynamoDB state table {self._store_name}', extra=dict(Func='Create', Op='StateStore'))
            self.__store = ddb.create_table(TableName=self._store_name,
                                            KeySchema=[
                                                {
                                                    'AttributeName': self._state_partition_key_cluster,
                                                    'KeyType': 'HASH'
                                                },
                                                {
                                                    'AttributeName': self._state_partition_key_rs,
                                                    'KeyType': 'RANGE'
                                                }
                                            ],
                                            AttributeDefinitions=[
                                                {
                                                    'AttributeName': self._state_partition_key_cluster,
                                                    'AttributeType': 'S'
                                                },
                                                {
                                                    'AttributeName': self._state_partition_key_rs,
                                                    'AttributeType': 'S'
                                                }
                                            ],
                                            BillingMode='PAY_PER_REQUEST'
                                            )
            self.__store.meta.client.get_waiter('table_exists').wait(TableName=self._store_name)

    def save_state(self, ldt: int, conn: str):
        """
        Commits state

        :param ldt: int. Last Document Time
        :param conn: str. Connection string of tail client.
        :return:
        """
        self.__store.update_item(Key={self._state_partition_key_cluster: self.state_key_cluster,
                                      self._state_partition_key_rs: self.state_key_replicaset},
                                 UpdateExpression='SET ldt=:ldt, updated_at=:upd, conn=:conn',
                                 ExpressionAttributeValues={':ldt': ldt,
                                                            ':upd': datetime.utcnow().isoformat(),
                                                            ':conn': conn}
                                 )

    def read_state_by_key(self):
        """
        Retrieves state for current MongoDB connection.

        :return:
        """
        resp = self.__store.get_item(
            Key={self._state_partition_key_cluster: self.state_key_cluster,
                 self._state_partition_key_rs: self.state_key_replicaset})
        if resp and 'Item' in resp:
            ldt = int_to_bson_timestamp(int(resp['Item']['ldt']))
            logger.debug(extra=dict(Func='State', Op='Read',
                                    Attributes={'cluster': self.state_key_cluster,
                                                'replica_set': self.state_key_replicaset,
                                                'store': self._store_name,
                                                'checkpoint': int(resp['Item']['ldt'])}), msg='')
            return ldt
        else:
            return None

    def read_all_state(self) -> list:
        """
        Retrieves the entire state store

        :return: list.
        """
        resp = self.__store.scan(ConsistentRead=True)
        if 'Items' in resp:
            return resp['Items']
