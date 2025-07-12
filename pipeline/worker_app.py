import datetime
import logging

import grpc
import pysftp
from celery import Celery
from celery import Task
from celery.signals import after_setup_logger

from pipeline.etl.state import ETLState
from pipeline.protos.transform_pb2 import InputFileRequest
from pipeline.protos.transform_pb2_grpc import TransformServiceStub
from pipeline.settings import settings

logger = logging.getLogger(__name__)


class TaskBase(Task):
    @staticmethod
    def connect_sftp(host_info: dict) -> pysftp.Connection:
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            client = pysftp.Connection(
                host=host_info["host"],
                username=host_info["username"],
                password=host_info["password"],
                port=host_info["port"],
                cnopts=cnopts,
            )
            return client
        except Exception as e:
            raise ConnectionError(f"Failed to establish SFTP connection: {str(e)}")

    @staticmethod
    def connect_grpc(etl_state: ETLState):
        options = []
        options.append(("grpc.service_config", settings.GRPC_SERVICE_CONFIG))
        try:
            with grpc.insecure_channel(settings.GRPC_URL, options=options) as channel:
                stub = TransformServiceStub(channel)
                response = stub.ProcessNYCTrip(
                    InputFileRequest(
                        input_file=etl_state.local_file_path,
                        remote_file_path=etl_state.remote_file_path,
                    ),
                )
            return response
        except grpc.RpcError as rpc_error:
            raise grpc.RpcError(str(rpc_error))


class CeleryConfig:
    accept_content = ["json", "pickle"]
    broker_pool_limit = 50
    broker_url = settings.RABBITMQ_URL
    result_backend = settings.REDIS_URL
    result_serializer = "pickle"
    result_expires = datetime.timedelta(hours=1)
    task_compression = "gzip"
    task_reject_on_worker_lost = True
    task_serializer = "pickle"
    task_queue_max_priority = 10
    task_default_priority = 1
    imports = (
        "pipeline.etl.etl",
        "pipeline.tests.test_connection",
    )
    timezone = "Asia/Jakarta"
    worker_send_task_events = True
    task_send_sent_event = True
    broker_heartbeat = None


app = Celery()
app.config_from_object(CeleryConfig)


@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # add filehandler
    fh = logging.FileHandler("logs.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
