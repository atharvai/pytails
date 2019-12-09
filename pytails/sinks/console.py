from bson import json_util
from bson.json_util import JSONOptions, DatetimeRepresentation, JSONMode

import logging
from .sink import Sink

logger = logging.getLogger(__name__)


class ConsoleSink(Sink):

    def write_record(self, obj: dict) -> None:
        """
        Prints the document via a INFO log

        :param obj: dict.
        :return:
        """
        opts = JSONOptions(strict_number_long=False, datetime_representation=DatetimeRepresentation.ISO8601,
                           json_mode=JSONMode.RELAXED)
        obj_str = json_util.dumps(obj)
        logger.info(extra=dict(Func='Record', Op='Tail',
                               Attributes={'identifier': self.identifier,
                                           'record': obj_str}), msg=obj_str)
