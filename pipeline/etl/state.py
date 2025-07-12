import datetime

from pipeline.etl.config import ETLType


class ETLState:
    def __init__(self, etl_conf: ETLType):
        self.conf: ETLType = etl_conf
        self._start_process: datetime.datetime = datetime.datetime.now()
        self._finish_process: datetime.datetime | None = None
        self._remote_file_path: str | None = None
        self._local_file_path: str | None = None

    @property
    def total_process(self) -> float:
        self._finish_process = datetime.datetime.now()
        return (self._finish_process - self._start_process).total_seconds()

    @property
    def remote_file_path(self) -> str | None:
        return self._remote_file_path

    @remote_file_path.setter
    def remote_file_path(self, file_path) -> None:
        self._remote_file_path = file_path

    @property
    def local_file_path(self) -> str | None:
        return self._local_file_path

    @local_file_path.setter
    def local_file_path(self, file_path) -> None:
        self._local_file_path = file_path
