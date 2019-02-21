import abc


class StateStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def setup_store(self):
        pass

    @abc.abstractmethod
    def save_state(self, ldt: int, conn: str):
        pass

    @abc.abstractmethod
    def read_state_by_key(self):
        pass

    @abc.abstractmethod
    def read_all_state(self) -> list:
        pass
