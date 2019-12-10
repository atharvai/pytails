from .store import StateStore


class NullStore(StateStore):
    def setup_store(self):
        pass

    def save_state(self, ldt: int, conn: str):
        pass

    def read_state_by_key(self):
        pass

    def read_all_state(self) -> list:
        pass
