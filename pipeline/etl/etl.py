import datetime
import io
import logging
import os
import tarfile

from celery.utils.log import get_task_logger
from redis import Redis

from pipeline.etl.config import ETLType
from pipeline.etl.state import ETLState
from pipeline.settings import settings
from pipeline.worker_app import app
from pipeline.worker_app import TaskBase


logger: logging.Logger = get_task_logger(__name__)


class BytesIO(io.BytesIO):
    def delete(self):
        self.close()
        super().__init__(b"")


@app.task(base=TaskBase, bind=True)
def remote_check(self: TaskBase, etl_type: ETLType):
    logger.info(f"Type: {etl_type.type_str}, Start checking on remote files")
    path = etl_type.host_info["path"]
    file_contents = []

    sftp_client = self.connect_sftp(etl_type.host_info)
    for f_attr in sftp_client.listdir_attr(path):
        file_contents.append(os.path.join(str(path), f_attr.filename))

    sftp_client.close()

    for file_content in file_contents:
        if "test" in file_content:
            continue
        etl_state: ETLState = ETLState(etl_type)
        etl_state.remote_file_path = file_content
        app.signature("pipeline.etl.etl.extract").apply_async(
            kwargs={"etl_state": etl_state}, queue="etl"
        )

    logger.info(f"Type: {etl_type.type_str}, Finish checking on remote files")


@app.task(base=TaskBase, bind=True)
def extract(self: TaskBase, etl_state: ETLState):
    logger.info(
        f"Type: {etl_state.conf.type_str}, Start extracting file {etl_state.remote_file_path}"
    )
    output = BytesIO()
    source_file_path = etl_state.remote_file_path
    if source_file_path is None:
        raise ValueError("source_file_path cannot be empty")

    local_file_path = f"{self.request.id}-{os.path.basename(source_file_path)}"

    sftp_client = self.connect_sftp(etl_state.conf.host_info)
    try:
        sftp_client.getfo(source_file_path, output)  # type: ignore
    except IOError:
        raise Exception("File doesn't exist anymore in remote")
    sftp_client.close()

    comressed_filename = source_file_path.split("/")[-1]
    local_file_path = local_file_path.split(".")[0] + ".tar.gz"

    compressed_output = BytesIO()
    with tarfile.open(fileobj=compressed_output, mode="w:gz") as tar:
        tarinfo = tarfile.TarInfo(name=comressed_filename)
        tarinfo.size = output.getbuffer().nbytes

        output.seek(0)
        tar.addfile(tarinfo, output)

    output.delete()
    redis_client = Redis(host=settings.REDIS_HOST, db=settings.REDIS_DB)
    redis_client.set(
        local_file_path,
        compressed_output.getvalue(),
        ex=int(datetime.timedelta(hours=1).total_seconds()),
    )
    compressed_output.delete()
    redis_client.close()

    etl_state.local_file_path = local_file_path
    app.signature("pipeline.etl.etl.transform").apply_async(
        kwargs={"etl_state": etl_state}, queue="etl"
    )
    logger.info(
        f"Type: {etl_state.conf.type_str}, Finished extract file {etl_state.remote_file_path}"
    )


@app.task(base=TaskBase, bind=True)
def transform(self: TaskBase, etl_state: ETLState):
    logger.info(
        f"Type: {etl_state.conf.type_str}, Start transforming file {etl_state.remote_file_path}"
    )

    respose = self.connect_grpc(etl_state)
    print(f"total rows -> {respose.total_rows}")
    print(f"dropped rows -> {respose.dropped_rows}")
    print(f"processed rows -> {respose.processed_rows}")
    print(f"inserted rows -> {respose.inserted_rows}")
    print(f"Total ETL Duration -> {etl_state.total_process}")

    logger.info(
        f"Type: {etl_state.conf.type_str}, Finished transform file {etl_state.remote_file_path}"
    )
