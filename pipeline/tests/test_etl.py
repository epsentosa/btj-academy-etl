import uuid
from unittest.mock import patch

import grpc
import pytest
from redis import Redis

from pipeline.etl.config import ETLType
from pipeline.etl.etl import extract
from pipeline.etl.state import ETLState
from pipeline.protos.transform_pb2 import InputFileRequest
from pipeline.protos.transform_pb2 import InputFileTestRequest
from pipeline.protos.transform_pb2_grpc import TransformServiceStub
from pipeline.settings import settings
from pipeline.worker_app import app


@pytest.fixture(scope="session")
def etl_type():
    etl_type: ETLType = ETLType("NYCTrip")
    return etl_type


@pytest.fixture(scope="session")
def etl_state(etl_type):
    etl_state = ETLState(etl_type)
    etl_state.remote_file_path = "/upload/test-nyctrip.tsv"
    # etl_state.remote_file_path = "/upload/yellow-tripdata-2025-01.tsv"
    yield etl_state


@pytest.fixture(scope="session")
def grpc_channel():
    grpc_channel = grpc.insecure_channel(settings.GRPC_URL)
    yield grpc_channel
    grpc_channel.close()


# this is to configure that when running tests, it doesn't require to run celery directly
@pytest.fixture(autouse=True)
def configure_celery():
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    yield
    app.conf.task_always_eager = False
    app.conf.task_eager_propagates = False


def test_get_attribute_host_etl_type_success(etl_type):
    assert etl_type.host_info["host"] == "localhost"


def test_get_attribute_host_etl_type_fail():
    with pytest.raises(ValueError, match="ETL Type 'NYXTrip' isn't supported"):
        ETLType("NYXTrip")


def test_instantiate_etl_state(etl_type):
    etl_state: ETLState = ETLState(etl_type)
    assert etl_state.conf == etl_type


@pytest.mark.dependency()
def test_get_redis_key_after_extract(etl_state):
    with patch("pipeline.worker_app.app.signature"):
        task_id = str(uuid.uuid4())
        task = extract.apply_async(kwargs={"etl_state": etl_state}, task_id=task_id)
        task.wait()
        filename = etl_state.remote_file_path.split("/")[-1].split(".")[0]  # type: ignore
        etl_state.local_file_path = f"{task_id}-{filename}" + ".tar.gz"

        redis_client = Redis(host=settings.REDIS_HOST, db=settings.REDIS_DB)
        assert redis_client.exists(etl_state.local_file_path) == 1


@pytest.mark.dependency(depends=["test_get_redis_key_after_extract"])
def test_get_response_grpc_transform(grpc_channel, etl_state):
    location_id = 2
    stub = TransformServiceStub(grpc_channel)
    response = stub.ProcessTesting(
        InputFileTestRequest(
            location_id=location_id,
        ),
        timeout=10,
        wait_for_ready=True,
    )
    assert response.borough == "Queens"
    assert response.zone == "Jamaica Bay"
    assert response.service_zone == "Boro Zone"


@pytest.mark.dependency(depends=["test_get_redis_key_after_extract"])
def test_transform_load(grpc_channel, etl_state):
    stub = TransformServiceStub(grpc_channel)
    response = stub.ProcessNYCTrip(
        InputFileRequest(
            input_file=etl_state.local_file_path,
            remote_file_path=etl_state.remote_file_path,
        ),
        timeout=10,
        wait_for_ready=True,
    )
    assert response.total_rows == 100
