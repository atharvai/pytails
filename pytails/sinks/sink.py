import abc


class Sink(metaclass=abc.ABCMeta):
    identifier = None

    def __init__(self, identifier: str):
        self.identifier = identifier

    @abc.abstractmethod
    def write_record(self, obj: dict) -> None:
        """
        Method to write record to sink

        :param obj:
        """
        pass
