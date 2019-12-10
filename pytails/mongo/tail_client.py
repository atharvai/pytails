import abc
import platform
import signal
from datetime import datetime

from bson import Timestamp

from ..helpers.bson_utils import bson_timestamp_to_int
import logging
from ..sinks import Sink
from ..state import NullStore, DynamoDbStore
from ..state.store import StateStore

logger = logging.getLogger(__name__)


class TailClient(metaclass=abc.ABCMeta):
    __sigs_map = {v.value: k for k, v in signal.__dict__.items() if k.startswith('SIG')}
    _data_sinks = set()
    _checkpoint_store = None
    _client = None
    ts = Timestamp(datetime.utcnow(), 1)
    identifier = None

    def __init__(self, cluster: str, replica_set: str):
        self.identifier = cluster + ':' + replica_set
        self.__set_interrupt_handler()
        self.register_checkpoint_store(DynamoDbStore(cluster, replica_set))
        # self.register_checkpoint_store(NullStore())

    def checkpoint(self, doc: dict = None):
        logger.debug(extra=dict(Func='Checkpoint', Op='Tail',
                     Attributes={'identifier': self.identifier, 'host': self._client.address[0],
                                 'port': self._client.address[1],
                                 'checkpoint': bson_timestamp_to_int(self.ts)}), msg='')
        self._checkpoint_store.save_state(bson_timestamp_to_int(self.ts), str(self._client.address))

    def start_tail(self):
        self.tail()

    @abc.abstractmethod
    def tail(self):
        pass

    def stop_tail(self):
        """
        Commits a checkpoint.

        :return:
        """
        self.checkpoint()

        logger.info(extra=dict(Func='Stop', Op='Tail',
                    Attributes={'identifier': self.identifier, 'host': self._client.address[0],
                                'port': self._client.address[1]
                                }), msg='')

    def write_to_sink(self, doc: dict):
        """
        Writes document to all registered data sinks. Performs a checkpoint at the end.

        :param doc: dict
        :return:
        """
        for sink in self._data_sinks:
            sink.write_record(doc)

    def register_data_sink(self, sink: Sink):
        """
        Registers a data sink. Possibel to register multiple sinks by calling this method multiple times.

        :param sink: Sink.
        :return:
        """
        logger.info(extra=dict(Func='Register', Op='DataSink',
                     Attributes={'identifier': self.identifier, 'host': self._client.address,
                                 'port': self._client.address[1],
                                 'datasink': sink.__class__.__name__}), msg='')
        self._data_sinks.add(sink)

    def register_checkpoint_store(self, store: StateStore):
        """
        Registers a checkpoint store. Only one checkpoint store can be registered.

        :param store:
        :return:
        """
        self._checkpoint_store = store

    def sig_int_handler(self, signum: int, frame):
        """
        Signal interrupt handler. Calls stop_tail() to gracefully shutdown tailer.

        https://docs.python.org/3/library/signal.html#signal.signal

        :param signum: int.
        :param frame: frame objects
        :return:
        """
        logger.debug(extra=dict(Func='Signal', Op='Process',
                     Attributes={'identifier': self.identifier, 'host': self._client.address[0],
                                 'port': self._client.address[1],
                                 'signal': self.__sigs_map[signum],
                                 'signum': signum}), msg='')
        self.stop_tail()

    def sig_usr1_handler(self, signum, frame):
        """
        SIGUSR1 signal handler. Performs a checkpoint.

        :param signum:
        :param frame:
        :return:
        """
        logger.debug(extra=dict(Func='Signal', Op='Process',
                     Attributes={'identifier': self.identifier, 'host': self._client.address[0],
                                 'port': self._client.address[1],
                                 'signal': self.__sigs_map[signum],
                                 'signum': signum}), msg='')
        self.checkpoint()

    def __set_interrupt_handler(self):
        """
        Registers signal interrupt handlers for SIGINT, SIGTERM to stop tailing. and SIGUSR1 (non Windows) to checkpoint.
        
        :return:
        """
        signals = [signal.SIGINT,
                   signal.SIGTERM
                   ]
        for s in signals:
            signal.signal(s, self.sig_int_handler)

        if platform.system() != 'Windows':
            signal.signal(signal.SIGUSR1, self.sig_usr1_handler)
