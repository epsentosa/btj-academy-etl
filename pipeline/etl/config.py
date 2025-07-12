from abc import abstractmethod
from typing import Any

from pipeline.db.session import get_session
from pipeline.db.table import ETLConfig


class _ConfBase:
    @property
    @abstractmethod
    def db_table(self) -> str:
        raise AttributeError


class ETLType:
    class configs:
        class NYCTrip(_ConfBase):
            db_table = "nyc_trip"

    def __init__(self, etl_type: str):
        self.type_str = etl_type
        self.conf = self.__get_config()
        self.host_info = self.__get_host_info()

    def __get_config(self) -> _ConfBase:
        try:
            cls = getattr(ETLType.configs, self.type_str)
            if not isinstance(cls, type):
                raise ValueError(f"ETL Type '{self.type_str}' isn't supported")
            return cls()
        except AttributeError:
            raise ValueError(f"ETL Type '{self.type_str}' isn't supported")

    def __get_host_info(self) -> dict[str, Any]:
        with get_session() as session:
            result = (
                session.query(ETLConfig)
                .filter(ETLConfig.name == self.type_str)
                .one_or_none()
            )
            if result is None:
                raise ValueError(f"ETL Type '{self.type_str}' hasn't host config")
            return {
                "method": result.method,
                "host": result.host,
                "port": result.port,
                "username": result.username,
                "password": result.password,
                "path": result.path,
            }
