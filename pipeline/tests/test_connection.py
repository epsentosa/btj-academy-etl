import time

import pytest
from kombu import Connection
from redis import Redis
from sqlalchemy import text

from pipeline.db.session import get_session
from pipeline.settings import settings
from pipeline.worker_app import app


def test_postgres_connection():
    with get_session() as db:
        try:
            result = db.execute(text("SELECT 1"))
            assert result.scalar() == 1
        except Exception as e:
            pytest.fail(f"Database connection failed: {str(e)}")


def test_redis_connection():
    try:
        redis_client = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )
        result = redis_client.ping()
        assert result
    except Exception as e:
        pytest.fail(f"Redis connection failed: {str(e)}")
    finally:
        redis_client.close()


def test_rabbitmq_connection():
    try:
        with Connection(settings.RABBITMQ_URL) as conn:
            conn.connect()
            assert conn.connected
    except Exception as e:
        pytest.fail(f"Failed to connect to RabbitMQ: {str(e)}")


@pytest.fixture(autouse=True)
def configure_celery():
    app.conf.task_always_eager = True  # Run tasks synchronously
    app.conf.task_eager_propagates = True
    yield
    app.conf.task_always_eager = False  # Reset after test
    app.conf.task_eager_propagates = False


@app.task
def dummy_task(x, y):
    return x + y


def test_celery_result_backend_connection():
    try:
        task = dummy_task.apply_async(args=(5, 4), queue="etl")
        start_time = time.time()
        while not task.ready() and (time.time() - start_time) < 10:
            time.sleep(0.1)

        if not task.ready():
            pytest.fail("Task did not complete within timeout")

        assert task.status == "SUCCESS"
        result = task.get(timeout=1)
        assert result == 9
    except Exception as e:
        pytest.fail(f"Failed to connect to or retrieve from result backend: {e}")
