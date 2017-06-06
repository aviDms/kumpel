from abc import ABCMeta, abstractmethod


class BaseConnector(metaclass=ABCMeta):
    """
    Abstract class for a connector.
    Each connector should have the following methods:
    """
    @abstractmethod
    def test_connection(self):
        raise NotImplementedError

    def __str__(self):
        pass

    def __repr__(self):
        pass


class SQLConnector(BaseConnector):
    @abstractmethod
    def create_table(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def drop_table(self):
        raise NotImplementedError

    @abstractmethod
    def truncate_table(self):
        raise NotImplementedError

    @abstractmethod
    def read_query(self):
        raise NotImplementedError

    @abstractmethod
    def read_table(self):
        raise NotImplementedError

    @abstractmethod
    def write_to_table(self):
        raise NotImplementedError

    @abstractmethod
    def write_query_to_table(self):
        raise NotImplementedError
