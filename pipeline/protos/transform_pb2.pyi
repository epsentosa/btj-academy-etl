import datetime
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InputFileRequest(_message.Message):
    __slots__ = ("input_file", "remote_file_path")
    INPUT_FILE_FIELD_NUMBER: _ClassVar[int]
    REMOTE_FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    input_file: str
    remote_file_path: str
    def __init__(
        self, input_file: _Optional[str] = ..., remote_file_path: _Optional[str] = ...
    ) -> None: ...

class ProcessFileResponse(_message.Message):
    __slots__ = (
        "total_rows",
        "dropped_rows",
        "processed_rows",
        "inserted_rows",
        "max_time",
        "min_time",
    )
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    DROPPED_ROWS_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_ROWS_FIELD_NUMBER: _ClassVar[int]
    INSERTED_ROWS_FIELD_NUMBER: _ClassVar[int]
    MAX_TIME_FIELD_NUMBER: _ClassVar[int]
    MIN_TIME_FIELD_NUMBER: _ClassVar[int]
    total_rows: int
    dropped_rows: int
    processed_rows: int
    inserted_rows: int
    max_time: _timestamp_pb2.Timestamp
    min_time: _timestamp_pb2.Timestamp
    def __init__(
        self,
        total_rows: _Optional[int] = ...,
        dropped_rows: _Optional[int] = ...,
        processed_rows: _Optional[int] = ...,
        inserted_rows: _Optional[int] = ...,
        max_time: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
        min_time: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
    ) -> None: ...

class InputFileTestRequest(_message.Message):
    __slots__ = ("location_id",)
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    location_id: int
    def __init__(self, location_id: _Optional[int] = ...) -> None: ...

class ProcessFileTestResponse(_message.Message):
    __slots__ = ("borough", "zone", "service_zone")
    BOROUGH_FIELD_NUMBER: _ClassVar[int]
    ZONE_FIELD_NUMBER: _ClassVar[int]
    SERVICE_ZONE_FIELD_NUMBER: _ClassVar[int]
    borough: str
    zone: str
    service_zone: str
    def __init__(
        self,
        borough: _Optional[str] = ...,
        zone: _Optional[str] = ...,
        service_zone: _Optional[str] = ...,
    ) -> None: ...
